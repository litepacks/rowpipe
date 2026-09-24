import mysql from "mysql2/promise";
import type { DataBatch, Row } from "../../core/types.js";
import { escapeIdentifier, escapeQualifiedTable, generateCreateTableSql, mysqlTypeToRowpipe } from "../mapping.js";
import type {
  DatabaseAdapter,
  DatabaseConnectionConfig,
  DatabaseDialect,
  DatabaseReadOptions,
  DatabaseWriteOptions,
  DatabaseWriteResult,
  TableColumnMetadata,
  TableSchemaMetadata,
} from "../types.js";

export class MysqlDatabaseAdapter implements DatabaseAdapter {
  readonly dialect: DatabaseDialect = "mysql";
  readonly sanitizedUrl: string;
  private connectionConfig: DatabaseConnectionConfig;
  private pool: mysql.Pool;

  constructor(config: DatabaseConnectionConfig) {
    this.sanitizedUrl = config.sanitizedUrl;
    this.connectionConfig = config;
    this.pool = mysql.createPool({
      host: config.host || "localhost",
      port: config.port || 3306,
      user: config.user || "root",
      password: config.password,
      database: config.database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
      supportBigNumbers: true,
      bigNumberStrings: true,
      dateStrings: true,
    });
  }

  async *stream(options: DatabaseReadOptions): AsyncIterable<DataBatch> {
    const batchSize = Math.max(1, options.batchSize || 1000);
    const sql = options.query || (options.table ? `SELECT * FROM ${escapeQualifiedTable(options.table, "mysql")}` : "");
    if (!sql) {
      throw new Error("Either table or query must be specified for MySQL stream");
    }

    const conn = await this.pool.getConnection();

    try {
      // mysql2 underlying connection stream
      const rawConn: any = (conn as any).connection;
      const queryStream = rawConn.query(sql, options.params || []).stream({ highWaterMark: batchSize });

      let currentBatch: Row[] = [];
      let offset = 0;

      const onAbort = () => {
        queryStream.destroy();
      };

      if (options.signal) {
        options.signal.addEventListener("abort", onAbort, { once: true });
      }

      try {
        for await (const row of queryStream) {
          if (options.signal?.aborted) {
            break;
          }

          currentBatch.push(row as Row);

          if (currentBatch.length >= batchSize) {
            yield {
              rows: currentBatch,
              offset,
            };
            offset += currentBatch.length;
            currentBatch = [];
          }
        }

        if (currentBatch.length > 0 && !options.signal?.aborted) {
          yield {
            rows: currentBatch,
            offset,
          };
        }
      } finally {
        if (options.signal) {
          options.signal.removeEventListener("abort", onAbort);
        }
        queryStream.destroy();
      }
    } finally {
      conn.release();
    }
  }

  async write(
    input: AsyncIterable<DataBatch>,
    options: DatabaseWriteOptions
  ): Promise<DatabaseWriteResult> {
    const startTime = Date.now();
    let rowsWritten = 0;
    let batchesWritten = 0;
    let tableCreated = false;

    const table = options.table;
    if (!table) {
      throw new Error("Target table name is required for database write");
    }

    const escapedTable = escapeQualifiedTable(table, "mysql");
    const conn = await this.pool.getConnection();

    try {
      if (options.truncate) {
        await conn.query(`TRUNCATE TABLE ${escapedTable};`);
      }

      const useTx = options.transaction !== false;
      let insertColumns: string[] = [];

      for await (const batch of input) {
        if (batch.rows.length === 0) continue;

        // Auto create table from first batch if requested
        if (options.createTable && !tableCreated) {
          const firstRow = batch.rows[0]!;
          const inferredSchema: Record<string, any> = {};
          for (const [key, val] of Object.entries(firstRow)) {
            if (typeof val === "number") {
              inferredSchema[key] = Number.isInteger(val) ? "integer" : "number";
            } else if (typeof val === "bigint") {
              inferredSchema[key] = "bigint";
            } else if (typeof val === "boolean") {
              inferredSchema[key] = "boolean";
            } else {
              inferredSchema[key] = "string";
            }
          }
          const ddl = generateCreateTableSql(table, inferredSchema, "mysql", options.conflictColumns);
          if (options.dryRun) {
            process.stdout.write(`\n-- Dry-run DDL:\n${ddl}\n`);
            return { rowsWritten: 0, batchesWritten: 0, durationMs: Date.now() - startTime, tableCreated: true };
          }
          await conn.query(ddl);
          tableCreated = true;
        }

        if (insertColumns.length === 0) {
          insertColumns = Object.keys(batch.rows[0]!);
        }

        if (useTx) {
          await conn.beginTransaction();
        }

        try {
          const escapedCols = insertColumns.map((c) => escapeIdentifier(c, "mysql")).join(", ");
          const MAX_PARAMS = 50000;
          const chunkSize = Math.max(1, Math.floor(MAX_PARAMS / insertColumns.length));

          for (let i = 0; i < batch.rows.length; i += chunkSize) {
            const rowChunk = batch.rows.slice(i, i + chunkSize);
            const rowPlaceholder = `(${insertColumns.map(() => "?").join(", ")})`;
            const allPlaceholders = rowChunk.map(() => rowPlaceholder).join(", ");
            const flatParams: unknown[] = [];

            for (const row of rowChunk) {
              for (const col of insertColumns) {
                const v = row[col];
                if (typeof v === "object" && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date)) {
                  flatParams.push(JSON.stringify(v));
                } else {
                  flatParams.push(v === undefined ? null : v);
                }
              }
              rowsWritten++;
            }

            let sql = `INSERT INTO ${escapedTable} (${escapedCols}) VALUES ${allPlaceholders}`;

            if (options.upsert) {
              const updateCols = insertColumns
                .map((c) => `${escapeIdentifier(c, "mysql")} = VALUES(${escapeIdentifier(c, "mysql")})`)
                .join(", ");
              sql += ` ON DUPLICATE KEY UPDATE ${updateCols}`;
            }

            await conn.query(sql, flatParams);
          }

          if (useTx) {
            await conn.commit();
          }
        } catch (err) {
          if (useTx) {
            try {
              await conn.rollback();
            } catch {
              // Ignore rollback errors
            }
          }
          throw err;
        }

        batchesWritten++;
      }
    } finally {
      conn.release();
    }

    return {
      rowsWritten,
      batchesWritten,
      durationMs: Date.now() - startTime,
      tableCreated,
    };
  }

  async getTables(): Promise<string[]> {
    const [rows]: any = await this.pool.query(
      `SELECT table_name
       FROM information_schema.tables
       WHERE table_schema = DATABASE()
         AND table_type = 'BASE TABLE'
       ORDER BY table_name`
    );
    return rows.map((r: any) => r.table_name || r.TABLE_NAME);
  }

  async getTableSchema(table: string): Promise<TableSchemaMetadata> {
    const [rows]: any = await this.pool.query(
      `SELECT column_name, column_type, is_nullable, column_key, column_default
       FROM information_schema.columns
       WHERE table_schema = DATABASE() AND table_name = ?
       ORDER BY ordinal_position`,
      [table]
    );

    const primaryKeys: string[] = [];
    const columns: TableColumnMetadata[] = rows.map((r: any) => {
      const colName = r.column_name || r.COLUMN_NAME;
      const colType = r.column_type || r.COLUMN_TYPE;
      const isPk = (r.column_key || r.COLUMN_KEY) === "PRI";
      if (isPk) primaryKeys.push(colName);

      return {
        name: colName,
        dbType: colType,
        rowpipeType: mysqlTypeToRowpipe(colType),
        nullable: (r.is_nullable || r.IS_NULLABLE) === "YES",
        isPrimaryKey: isPk,
        defaultValue: r.column_default || r.COLUMN_DEFAULT,
      };
    });

    return {
      table,
      columns,
      primaryKeys,
    };
  }

  async executeRaw(sql: string, params?: unknown[]): Promise<unknown> {
    const [result] = await this.pool.query(sql, params);
    return result;
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}
