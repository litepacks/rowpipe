import type { DataStream, TabularWriter, WriterOptions } from "../core/types.js";
import { createDatabaseAdapter } from "./adapters/index.js";
import type { DatabaseAdapter, DatabaseConnectionConfig, DatabaseWriteOptions, DatabaseWriteResult } from "./types.js";

export class DatabaseWriter implements TabularWriter {
  private adapter: DatabaseAdapter;
  private defaultOptions: DatabaseWriteOptions;
  private lastResult?: DatabaseWriteResult;

  constructor(
    urlOrConfig: string | DatabaseConnectionConfig | DatabaseAdapter,
    defaultOptions: DatabaseWriteOptions = { table: "" }
  ) {
    if (typeof urlOrConfig === "object" && "write" in urlOrConfig) {
      this.adapter = urlOrConfig as DatabaseAdapter;
    } else {
      this.adapter = createDatabaseAdapter(urlOrConfig as string | DatabaseConnectionConfig);
    }
    this.defaultOptions = defaultOptions;
  }

  getAdapter(): DatabaseAdapter {
    return this.adapter;
  }

  getLastResult(): DatabaseWriteResult | undefined {
    return this.lastResult;
  }

  get dialect() {
    return this.adapter.dialect;
  }

  get sanitizedUrl() {
    return this.adapter.sanitizedUrl;
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const table = options?.table || this.defaultOptions.table;
    if (!table) {
      throw new Error("Target table name is required for database write");
    }

    const effectiveOptions: DatabaseWriteOptions = {
      ...this.defaultOptions,
      table,
      transaction: options?.transaction ?? this.defaultOptions.transaction,
      createTable: options?.createTable ?? this.defaultOptions.createTable,
      upsert: options?.upsert ?? this.defaultOptions.upsert,
      conflictColumns: options?.conflictColumns ?? this.defaultOptions.conflictColumns,
      truncate: options?.truncate ?? this.defaultOptions.truncate,
    };

    this.lastResult = await this.adapter.write(dataStream, effectiveOptions);
  }

  async close(): Promise<void> {
    await this.adapter.close();
  }
}
