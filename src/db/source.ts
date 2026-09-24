import type { DataStream, InspectionMetadata, ReaderOptions, TabularReader } from "../core/types.js";
import { createDatabaseAdapter } from "./adapters/index.js";
import { parseDatabaseUrl } from "./url.js";
import type { DatabaseAdapter, DatabaseConnectionConfig, DatabaseReadOptions } from "./types.js";

export class DatabaseReader implements TabularReader {
  private adapter: DatabaseAdapter;
  private defaultOptions: DatabaseReadOptions;

  constructor(
    urlOrConfig: string | DatabaseConnectionConfig | DatabaseAdapter,
    defaultOptions: DatabaseReadOptions = {}
  ) {
    if (typeof urlOrConfig === "object" && "stream" in urlOrConfig) {
      this.adapter = urlOrConfig as DatabaseAdapter;
      this.defaultOptions = { ...defaultOptions };
    } else {
      const config = typeof urlOrConfig === "string" ? parseDatabaseUrl(urlOrConfig) : urlOrConfig;
      this.adapter = createDatabaseAdapter(config);
      this.defaultOptions = {
        table: config.queryOptions?.["table"] || defaultOptions.table,
        query: config.queryOptions?.["query"] || defaultOptions.query,
        ...defaultOptions,
      };
    }
  }

  getAdapter(): DatabaseAdapter {
    return this.adapter;
  }

  get dialect() {
    return this.adapter.dialect;
  }

  get sanitizedUrl() {
    return this.adapter.sanitizedUrl;
  }

  read(options?: ReaderOptions): DataStream {
    const effectiveOptions: DatabaseReadOptions = {
      ...this.defaultOptions,
      table: options?.table || this.defaultOptions.table,
      query: options?.query || this.defaultOptions.query,
      params: options?.params || this.defaultOptions.params,
      batchSize: options?.batchSize || this.defaultOptions.batchSize || 1000,
      signal: options?.signal || this.defaultOptions.signal,
    };

    return this.adapter.stream(effectiveOptions);
  }

  async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    const targetTable = options?.table || this.defaultOptions.table;

    if (targetTable) {
      const schema = await this.adapter.getTableSchema(targetTable);
      return {
        format: `database (${this.adapter.dialect})`,
        columnCount: schema.columns.length,
        columns: schema.columns.map((c) => ({
          name: c.name,
          type: c.rowpipeType,
          nullPercentage: c.nullable ? 0 : 0,
        })),
      };
    }

    const tables = await this.adapter.getTables();
    return {
      format: `database (${this.adapter.dialect})`,
      sheetsCount: tables.length,
      sheets: tables.map((t) => ({
        name: t,
        rowCount: 0,
      })),
    };
  }

  async close(): Promise<void> {
    await this.adapter.close();
  }
}
