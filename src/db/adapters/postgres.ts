import pg from "pg";
import QueryStream from "pg-query-stream";
import type { DataBatch, Row } from "../../core/types.js";
import { escapeIdentifier, escapeQualifiedTable, generateCreateTableSql, postgresTypeToRowpipe } from "../mapping.js";
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

export class PostgresDatabaseAdapter implements DatabaseAdapter {
  readonly dialect: DatabaseDialect = "postgres";
  readonly sanitizedUrl: string;
  private client: pg.Client;
  private isConnected = false;

  constructor(config: DatabaseConnectionConfig) {
    this.sanitizedUrl = config.sanitizedUrl;
    this.client = new pg.Client({
      connectionString: config.url,
      ssl: config.ssl,
    });
  }

  private async ensureConnected(): Promise<void> {
    if (!this.isConnected) {
      await this.client.connect();
      this.isConnected = true;
    }
  }

  async *stream(options: DatabaseReadOptions): AsyncIterable<DataBatch> {
    await this.ensureConnected();

    const batchSize = Math.max(1, options.batchSize || 1000);
    const sql = options.query || (options.table ? `SELECT * FROM ${escapeQualifiedTable(options.table, "postgres")}` : "");
    if (!sql) {
      throw new Error("Either table or query must be specified for PostgreSQL stream");
    }

    const params = options.params || [];
    const queryStream = new QueryStream(sql, params, { batchSize });
    const stream = this.client.query(queryStream);

    let currentBatch: Row[] = [];
    let offset = 0;

    const onAbort = () => {
      stream.destroy();
    };

    if (options.signal) {
      options.signal.addEventListener("abort", onAbort, { once: true });
    }

    try {
      for await (const row of stream) {
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
      stream.destroy();
    }
  }

  async write(
    input: AsyncIterable<DataBatch>,
    options: DatabaseWriteOptions
  ): Promise<DatabaseWriteResult> {
    await this.ensureConnected();

    const startTime = Date.now();
    let rowsWritten = 0;
    let batchesWritten = 0;
    let tableCreated = false;

    const table = options.table;
    if (!table) {
      throw new Error("Target table name is required for database write");
    }

    const escapedTable = escapeQualifiedTable(table, "postgres");

    if (options.truncate) {
      await this.client.query(`TRUNCATE TABLE ${escapedTable};`);
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
        const ddl = generateCreateTableSql(table, inferredSchema, "postgres", options.conflictColumns);
        if (options.dryRun) {
          process.stdout.write(`\n-- Dry-run DDL:\n${ddl}\n`);
          return { rowsWritten: 0, batchesWritten: 0, durationMs: Date.now() - startTime, tableCreated: true };
        }
        await this.client.query(ddl);
        tableCreated = true;
      }

      if (insertColumns.length === 0) {
        insertColumns = Object.keys(batch.rows[0]!);
      }

      if (useTx) {
        await this.client.query("BEGIN;");
      }

      try {
        const escapedCols = insertColumns.map((c) => escapeIdentifier(c, "postgres")).join(", ");
        const MAX_PARAMS = 50000;
        const chunkSize = Math.max(1, Math.floor(MAX_PARAMS / insertColumns.length));

        for (let i = 0; i < batch.rows.length; i += chunkSize) {
          const rowChunk = batch.rows.slice(i, i + chunkSize);
          const valuesPlaceholderRows: string[] = [];
          const flatParams: unknown[] = [];
          let paramIdx = 1;

          for (const row of rowChunk) {
            const placeholders: string[] = [];
            for (const col of insertColumns) {
              const v = row[col];
              placeholders.push(`$${paramIdx++}`);
              if (typeof v === "object" && v !== null && !(v instanceof Uint8Array) && !(v instanceof Date)) {
                flatParams.push(JSON.stringify(v));
              } else {
                flatParams.push(v === undefined ? null : v);
              }
            }
            valuesPlaceholderRows.push(`(${placeholders.join(", ")})`);
            rowsWritten++;
          }

          let sql = `INSERT INTO ${escapedTable} (${escapedCols}) VALUES ${valuesPlaceholderRows.join(", ")}`;

          if (options.upsert && options.conflictColumns && options.conflictColumns.length > 0) {
            const conflictCols = options.conflictColumns.map((c) => escapeIdentifier(c, "postgres")).join(", ");
            const updateCols = insertColumns
              .filter((c) => !options.conflictColumns!.includes(c))
              .map((c) => `${escapeIdentifier(c, "postgres")} = EXCLUDED.${escapeIdentifier(c, "postgres")}`)
              .join(", ");

            if (updateCols.length > 0) {
              sql += ` ON CONFLICT (${conflictCols}) DO UPDATE SET ${updateCols}`;
            } else {
              sql += ` ON CONFLICT (${conflictCols}) DO NOTHING`;
            }
          }

          await this.client.query(sql, flatParams);
        }

        if (useTx) {
          await this.client.query("COMMIT;");
        }
      } catch (err) {
        if (useTx) {
          try {
            await this.client.query("ROLLBACK;");
          } catch {
            // Ignore rollback errors
          }
        }
        throw err;
      }

      batchesWritten++;
    }

    return {
      rowsWritten,
      batchesWritten,
      durationMs: Date.now() - startTime,
      tableCreated,
    };
  }

  async getTables(): Promise<string[]> {
    await this.ensureConnected();
    const res = await this.client.query(
      `SELECT table_schema || '.' || table_name AS full_name
       FROM information_schema.tables
       WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
         AND table_type = 'BASE TABLE'
       ORDER BY table_schema, table_name`
    );
    return res.rows.map((r: any) => r.full_name);
  }

  async getTableSchema(table: string): Promise<TableSchemaMetadata> {
    await this.ensureConnected();

    let schemaName = "public";
    let tableName = table;
    if (table.includes(".")) {
      const parts = table.split(".");
      schemaName = parts[0]!;
      tableName = parts[1]!;
    }

    const res = await this.client.query(
      `SELECT column_name, udt_name, data_type, is_nullable, column_default
       FROM information_schema.columns
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [schemaName, tableName]
    );

    const pkRes = await this.client.query(
      `SELECT kcu.column_name
       FROM information_schema.table_constraints tc
       JOIN information_schema.key_column_usage kcu
         ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
       WHERE tc.constraint_type = 'PRIMARY KEY'
         AND tc.table_schema = $1 AND tc.table_name = $2`,
      [schemaName, tableName]
    );

    const primaryKeys = pkRes.rows.map((r: any) => r.column_name);

    const columns: TableColumnMetadata[] = res.rows.map((r: any) => ({
      name: r.column_name,
      dbType: r.udt_name || r.data_type,
      rowpipeType: postgresTypeToRowpipe(r.udt_name || r.data_type),
      nullable: r.is_nullable === "YES",
      isPrimaryKey: primaryKeys.includes(r.column_name),
      defaultValue: r.column_default,
    }));

    return {
      table,
      columns,
      primaryKeys,
    };
  }

  async executeRaw(sql: string, params?: unknown[]): Promise<unknown> {
    await this.ensureConnected();
    return this.client.query(sql, params);
  }

  async close(): Promise<void> {
    if (this.isConnected) {
      await this.client.end();
      this.isConnected = false;
    }
  }
}
