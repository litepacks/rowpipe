import { InvalidArgumentError } from "../core/errors.js";
import type { DatabaseConnectionConfig, DatabaseDialect } from "./types.js";

/**
 * Checks if a string represents a database URL or SQLite path.
 */
export function isDatabaseUrl(input: string): boolean {
  if (!input || typeof input !== "string") return false;
  const trimmed = input.trim();
  if (
    trimmed.startsWith("postgres://") ||
    trimmed.startsWith("postgresql://") ||
    trimmed.startsWith("mysql://") ||
    trimmed.startsWith("sqlite://") ||
    trimmed.startsWith("sqlite:")
  ) {
    return true;
  }
  // File path ending in .db, .sqlite, .sqlite3 if explicitly marked or passed to db commands
  if (
    (trimmed.endsWith(".db") || trimmed.endsWith(".sqlite") || trimmed.endsWith(".sqlite3")) &&
    (trimmed.startsWith("./") || trimmed.startsWith("../") || trimmed.startsWith("/"))
  ) {
    return true;
  }
  return false;
}

/**
 * Sanitizes connection URLs by masking credentials (passwords).
 * Never emits raw passwords in logs, errors, or explain output.
 */
export function sanitizeConnectionString(urlStr: string): string {
  if (!urlStr || typeof urlStr !== "string") return "";
  const trimmed = urlStr.trim();

  // SQLite URL or file path
  if (trimmed.startsWith("sqlite://") || trimmed.startsWith("sqlite:") || !trimmed.includes("://")) {
    return trimmed;
  }

  try {
    const parsed = new URL(trimmed);
    if (parsed.password) {
      parsed.password = "***";
    }
    return parsed.toString();
  } catch {
    // Regex fallback
    return trimmed.replace(/(:[^:@/]+)(@)/, ":***$2");
  }
}

/**
 * Parses and validates database connection URLs into structured DatabaseConnectionConfig.
 */
export function parseDatabaseUrl(rawUrl: string): DatabaseConnectionConfig {
  const trimmed = rawUrl.trim();
  const sanitizedUrl = sanitizeConnectionString(trimmed);

  if (
    trimmed.startsWith("sqlite://") ||
    trimmed.startsWith("sqlite:") ||
    trimmed.endsWith(".db") ||
    trimmed.endsWith(".sqlite") ||
    trimmed.endsWith(".sqlite3")
  ) {
    let filePath = trimmed;
    if (filePath.startsWith("sqlite://")) {
      filePath = filePath.slice(9);
    } else if (filePath.startsWith("sqlite:")) {
      filePath = filePath.slice(7);
    }
    let queryOptions: Record<string, string> = {};
    const qIdx = filePath.indexOf("?");
    if (qIdx >= 0) {
      const search = filePath.slice(qIdx + 1);
      filePath = filePath.slice(0, qIdx);
      const params = new URLSearchParams(search);
      params.forEach((val, key) => {
        queryOptions[key] = val;
      });
    }

    return {
      dialect: "sqlite",
      url: trimmed,
      sanitizedUrl,
      filePath: filePath || ":memory:",
      queryOptions,
    };
  }

  if (trimmed.startsWith("postgres://") || trimmed.startsWith("postgresql://")) {
    let parsed: URL;
    try {
      parsed = new URL(trimmed);
    } catch (err: any) {
      throw new InvalidArgumentError(`Invalid PostgreSQL connection URL: ${err.message}`);
    }

    const host = parsed.hostname || "localhost";
    const port = parsed.port ? Number.parseInt(parsed.port, 10) : 5432;
    const database = parsed.pathname ? parsed.pathname.replace(/^\//, "") : undefined;
    const user = parsed.username ? decodeURIComponent(parsed.username) : undefined;
    const password = parsed.password ? decodeURIComponent(parsed.password) : undefined;

    const queryOptions: Record<string, string> = {};
    parsed.searchParams.forEach((val, key) => {
      queryOptions[key] = val;
    });

    const ssl = queryOptions["sslmode"] === "disable" ? false : queryOptions["ssl"] === "true" || undefined;

    return {
      dialect: "postgres",
      url: trimmed,
      sanitizedUrl,
      host,
      port,
      database,
      user,
      password,
      ssl,
      queryOptions,
    };
  }

  if (trimmed.startsWith("mysql://")) {
    let parsed: URL;
    try {
      parsed = new URL(trimmed);
    } catch (err: any) {
      throw new InvalidArgumentError(`Invalid MySQL connection URL: ${err.message}`);
    }

    const host = parsed.hostname || "localhost";
    const port = parsed.port ? Number.parseInt(parsed.port, 10) : 3306;
    const database = parsed.pathname ? parsed.pathname.replace(/^\//, "") : undefined;
    const user = parsed.username ? decodeURIComponent(parsed.username) : undefined;
    const password = parsed.password ? decodeURIComponent(parsed.password) : undefined;

    const queryOptions: Record<string, string> = {};
    parsed.searchParams.forEach((val, key) => {
      queryOptions[key] = val;
    });

    return {
      dialect: "mysql",
      url: trimmed,
      sanitizedUrl,
      host,
      port,
      database,
      user,
      password,
      queryOptions,
    };
  }

  throw new InvalidArgumentError(
    `Unsupported database URL dialect for "${sanitizedUrl}". Supported protocols: postgres://, postgresql://, mysql://, sqlite://`
  );
}
