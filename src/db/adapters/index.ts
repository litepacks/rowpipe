import { parseDatabaseUrl } from "../url.js";
import type { DatabaseAdapter, DatabaseConnectionConfig } from "../types.js";
import { SqliteDatabaseAdapter } from "./sqlite.js";
import { PostgresDatabaseAdapter } from "./postgres.js";
import { MysqlDatabaseAdapter } from "./mysql.js";

/**
 * Creates a concrete DatabaseAdapter instance from a connection URL or structured config.
 */
export function createDatabaseAdapter(
  urlOrConfig: string | DatabaseConnectionConfig
): DatabaseAdapter {
  const config = typeof urlOrConfig === "string" ? parseDatabaseUrl(urlOrConfig) : urlOrConfig;

  switch (config.dialect) {
    case "sqlite":
      return new SqliteDatabaseAdapter(config);

    case "postgres":
      return new PostgresDatabaseAdapter(config);

    case "mysql":
      return new MysqlDatabaseAdapter(config);

    default:
      throw new Error(`Unsupported database dialect: ${(config as any).dialect}`);
  }
}
