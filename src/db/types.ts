import type { ColumnType, DataBatch, Row } from "../core/types.js";

export type DatabaseDialect = "postgres" | "mysql" | "sqlite";

export interface DatabaseConnectionConfig {
  dialect: DatabaseDialect;
  url: string;
  sanitizedUrl: string;
  host?: string;
  port?: number;
  database?: string;
  user?: string;
  password?: string;
  ssl?: boolean | Record<string, unknown>;
  filePath?: string; // for SQLite
  queryOptions?: Record<string, string>;
}

export interface DatabaseReadOptions {
  table?: string;
  query?: string;
  params?: unknown[];
  batchSize?: number;
  signal?: AbortSignal;
  where?: string;
  select?: string[];
  sort?: Array<{ column: string; direction?: "asc" | "desc" }>;
  limit?: number;
  offset?: number;
  count?: boolean;
}

export interface DatabaseWriteOptions {
  table: string;
  batchSize?: number;
  transaction?: boolean;
  createTable?: boolean;
  upsert?: boolean;
  conflictColumns?: string[];
  truncate?: boolean;
  dryRun?: boolean;
}

export interface DatabaseWriteResult {
  rowsWritten: number;
  batchesWritten: number;
  durationMs: number;
  tableCreated?: boolean;
}

export interface TableColumnMetadata {
  name: string;
  dbType: string;
  rowpipeType: ColumnType;
  nullable: boolean;
  isPrimaryKey?: boolean;
  defaultValue?: unknown;
}

export interface TableSchemaMetadata {
  table: string;
  columns: TableColumnMetadata[];
  primaryKeys?: string[];
}

export interface DatabaseSourceMetadata {
  type: "database";
  dialect: DatabaseDialect;
  database?: string;
  table?: string;
  query?: string;
  sanitizedUrl: string;
}

export interface DatabaseAdapter {
  readonly dialect: DatabaseDialect;
  readonly sanitizedUrl: string;

  stream(options: DatabaseReadOptions): AsyncIterable<DataBatch>;
  write(input: AsyncIterable<DataBatch>, options: DatabaseWriteOptions): Promise<DatabaseWriteResult>;
  insertBatch?(table: string, rows: Row[], options?: DatabaseWriteOptions): Promise<DatabaseWriteResult>;
  getTables(): Promise<string[]>;
  getTableSchema(table: string): Promise<TableSchemaMetadata>;
  executeRaw(sql: string, params?: unknown[]): Promise<unknown>;
  close(): Promise<void>;
}
