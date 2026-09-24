import type { DataBatch, Row } from "../../core/types.js";
import { escapeIdentifier, escapeQualifiedTable, generateCreateTableSql, sqliteTypeToRowpipe } from "../mapping.js";
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

let cachedDatabaseSync: any = null;

async function getDatabaseSyncClass(): Promise<any> {
  if (cachedDatabaseSync) return cachedDatabaseSync;
  try {
    // Dynamic import to prevent startup module resolution failure on Node < 22.5.0
    const mod = await import("node:sqlite");
    cachedDatabaseSync = mod.DatabaseSync;
    return cachedDatabaseSync;
  } catch {
    throw new Error(
      `SQLite adapter requires Node.js >= 22.5.0 with built-in 'node:sqlite' support (current Node version: ${process.version}). ` +
      `Please run on Node.js 22.5+ to use SQLite databases with rowpipe, or connect to PostgreSQL / MySQL.`
    );
  }
}

function toSqliteParam(val: unknown): any {
  if (val === null || val === undefined) return null;
  if (typeof val === "number" || typeof val === "bigint" || typeof val === "string") return val;
  if (typeof val === "boolean") return val ? 1 : 0;
  if (val instanceof Date) return val.toISOString();
  if (Buffer.isBuffer(val) || val instanceof Uint8Array) return val;
  if (typeof val === "object") return JSON.stringify(val);
  return String(val);
}

export class SqliteDatabaseAdapter implements DatabaseAdapter {
  readonly dialect: DatabaseDialect = "sqlite";
  readonly config: DatabaseConnectionConfig;
  readonly sanitizedUrl: string;
  private db?: any;
  private isClosed = false;

  constructor(config: DatabaseConnectionConfig) {
    this.config = config;
    this.sanitizedUrl = config.sanitizedUrl;
  }

  private async getDb(): Promise<any> {
    if (this.isClosed) {
      throw new Error("SQLite database connection is closed");
    }
    if (!this.db) {
      const DatabaseSync = await getDatabaseSyncClass();
      const dbPath = this.config.filePath || ":memory:";
      this.db = new DatabaseSync(dbPath);
    }
    return this.db;
  }

  async *stream(options: DatabaseReadOptions): AsyncIterable<DataBatch> {
    const db = await this.getDb();
    const batchSize = Math.max(1, options.batchSize || 1000);
    const sql = options.query || (options.table ? `SELECT * FROM ${escapeQualifiedTable(options.table, "sqlite")}` : "");
    if (!sql) {
      throw new Error("Either table or query must be specified for SQLite stream");
    }

    const params = (options.params || []).map(toSqliteParam);
    const stmt = db.prepare(sql);
    if (typeof (stmt as any).setReadBigInts === "function") {
      (stmt as any).setReadBigInts(true);
    }
    let currentBatch: Row[] = [];
    let offset = 0;

    const rowsIterable: Iterable<any> =
      typeof (stmt as any).iterate === "function"
        ? (stmt as any).iterate(...params)
        : (stmt as any).all(...params);

    try {
      for (const rawRow of rowsIterable) {
        if (options.signal?.aborted) {
          break;
        }

        currentBatch.push(rawRow as Row);

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
      // Statement iteration cleanup is automatic in node:sqlite
    }
  }

  async write(
    input: AsyncIterable<DataBatch>,
    options: DatabaseWriteOptions
  ): Promise<DatabaseWriteResult> {
    const db = await this.getDb();
    const startTime = Date.now();
    let rowsWritten = 0;
    let batchesWritten = 0;
    let tableCreated = false;

    const table = options.table;
    if (!table) {
      throw new Error("Target table name is required for database write");
    }

    const escapedTable = escapeQualifiedTable(table, "sqlite");

    if (options.truncate) {
      db.exec(`DELETE FROM ${escapedTable};`);
    }

    const useTx = options.transaction !== false;

    let insertStmt: any = null;
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
        const ddl = generateCreateTableSql(table, inferredSchema, "sqlite", options.conflictColumns);
        if (options.dryRun) {
          process.stdout.write(`\n-- Dry-run DDL:\n${ddl}\n`);
          return { rowsWritten: 0, batchesWritten: 0, durationMs: Date.now() - startTime, tableCreated: true };
        }
        db.exec(ddl);
        tableCreated = true;
      }

      if (!insertStmt) {
        insertColumns = Object.keys(batch.rows[0]!);
        const escapedCols = insertColumns.map((c) => escapeIdentifier(c, "sqlite")).join(", ");
        const placeholders = insertColumns.map(() => "?").join(", ");

        let sql = `INSERT INTO ${escapedTable} (${escapedCols}) VALUES (${placeholders})`;

        if (options.upsert && options.conflictColumns && options.conflictColumns.length > 0) {
          const conflictCols = options.conflictColumns.map((c) => escapeIdentifier(c, "sqlite")).join(", ");
          const updateCols = insertColumns
            .filter((c) => !options.conflictColumns!.includes(c))
            .map((c) => `${escapeIdentifier(c, "sqlite")} = excluded.${escapeIdentifier(c, "sqlite")}`)
            .join(", ");

          if (updateCols.length > 0) {
            sql += ` ON CONFLICT (${conflictCols}) DO UPDATE SET ${updateCols}`;
          } else {
            sql += ` ON CONFLICT (${conflictCols}) DO NOTHING`;
          }
        }

        insertStmt = db.prepare(sql);
      }

      if (useTx) {
        db.exec("BEGIN TRANSACTION;");
      }

      try {
        for (const row of batch.rows) {
          const vals = insertColumns.map((c) => toSqliteParam(row[c]));
          insertStmt.run(...vals);
          rowsWritten++;
        }

        if (useTx) {
          db.exec("COMMIT;");
        }
      } catch (err) {
        if (useTx) {
          db.exec("ROLLBACK;");
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

  async insertBatch(
    table: string,
    rows: Row[],
    options?: DatabaseWriteOptions
  ): Promise<DatabaseWriteResult> {
    async function* singleBatch() {
      yield { rows, offset: 0 };
    }
    return this.write(singleBatch(), { table, ...options });
  }

  async getTables(): Promise<string[]> {
    const db = await this.getDb();
    const stmt = db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name ASC"
    );
    const rows = stmt.all() as Array<{ name: string }>;
    return rows.map((r) => r.name);
  }

  async getTableSchema(table: string): Promise<TableSchemaMetadata> {
    const db = await this.getDb();
    const stmt = db.prepare(`PRAGMA table_info(${escapeIdentifier(table, "sqlite")})`);
    const rows = stmt.all() as Array<{
      cid: number;
      name: string;
      type: string;
      notnull: number;
      dflt_value: unknown;
      pk: number;
    }>;

    const columns: TableColumnMetadata[] = rows.map((r) => ({
      name: r.name,
      dbType: r.type || "TEXT",
      rowpipeType: sqliteTypeToRowpipe(r.type),
      nullable: r.notnull === 0,
      isPrimaryKey: r.pk > 0,
      defaultValue: r.dflt_value,
    }));

    const primaryKeys = rows.filter((r) => r.pk > 0).map((r) => r.name);

    return {
      table,
      columns,
      primaryKeys,
    };
  }

  async executeRaw(sql: string, params?: unknown[]): Promise<unknown> {
    const db = await this.getDb();
    if (params && params.length > 0) {
      const stmt = db.prepare(sql);
      return stmt.all(...params.map(toSqliteParam));
    }
    return db.exec(sql);
  }

  async close(): Promise<void> {
    if (this.isClosed) return;
    this.isClosed = true;
    if (this.db) {
      try {
        this.db.close();
      } catch {
        // Ignore if already closed
      }
      this.db = undefined;
    }
  }
}
