import type { ColumnType } from "../core/types.js";
import type { DatabaseDialect } from "./types.js";

/**
 * Escapes a single SQL identifier (table/column name) safely according to dialect.
 */
export function escapeIdentifier(name: string, dialect: DatabaseDialect): string {
  const clean = name.trim();
  if (dialect === "mysql") {
    return `\`${clean.replace(/`/g, "``")}\``;
  }
  // Postgres and SQLite use double quotes
  return `"${clean.replace(/"/g, '""')}"`;
}

/**
 * Escapes a qualified table name (e.g. "public.users" -> `"public"."users"`).
 */
export function escapeQualifiedTable(table: string, dialect: DatabaseDialect): string {
  const parts = table.split(".");
  return parts.map((p) => escapeIdentifier(p, dialect)).join(".");
}

/**
 * Maps PostgreSQL column types to Rowpipe logical ColumnType.
 */
export function postgresTypeToRowpipe(dbType: string): ColumnType {
  const lower = dbType.toLowerCase().trim();
  if (lower.includes("int2") || lower.includes("int4") || lower.includes("smallint") || lower.includes("integer") || lower.includes("serial")) {
    return "integer";
  }
  if (lower.includes("int8") || lower.includes("bigint") || lower.includes("bigserial")) {
    return "bigint";
  }
  if (lower.includes("numeric") || lower.includes("decimal") || lower.includes("money")) {
    return "decimal";
  }
  if (lower.includes("float") || lower.includes("double") || lower.includes("real")) {
    return "number";
  }
  if (lower.includes("bool")) {
    return "boolean";
  }
  if (lower === "date") {
    return "date";
  }
  if (lower.includes("timestamp") || lower.includes("time")) {
    return "datetime";
  }
  if (lower.includes("json")) {
    return "json";
  }
  if (lower.includes("bytea") || lower.includes("blob")) {
    return "binary";
  }
  return "string";
}

/**
 * Maps MySQL column types to Rowpipe logical ColumnType.
 */
export function mysqlTypeToRowpipe(dbType: string): ColumnType {
  const lower = dbType.toLowerCase().trim();
  if (lower.includes("tinyint(1)") || lower === "boolean" || lower === "bool") {
    return "boolean";
  }
  if (lower.includes("smallint") || lower.includes("mediumint") || lower.includes("int") && !lower.includes("bigint")) {
    return "integer";
  }
  if (lower.includes("bigint")) {
    return "bigint";
  }
  if (lower.includes("decimal") || lower.includes("numeric")) {
    return "decimal";
  }
  if (lower.includes("float") || lower.includes("double")) {
    return "number";
  }
  if (lower === "date") {
    return "date";
  }
  if (lower.includes("datetime") || lower.includes("timestamp") || lower.includes("time")) {
    return "datetime";
  }
  if (lower.includes("json")) {
    return "json";
  }
  if (lower.includes("blob") || lower.includes("binary")) {
    return "binary";
  }
  return "string";
}

/**
 * Maps SQLite column types to Rowpipe logical ColumnType.
 */
export function sqliteTypeToRowpipe(dbType: string): ColumnType {
  const lower = (dbType || "text").toLowerCase().trim();
  if (lower.includes("int")) {
    if (lower.includes("bigint") || lower.includes("int8")) return "bigint";
    return "integer";
  }
  if (lower.includes("real") || lower.includes("floa") || lower.includes("doub")) {
    return "number";
  }
  if (lower.includes("dec") || lower.includes("num")) {
    return "decimal";
  }
  if (lower.includes("bool")) {
    return "boolean";
  }
  if (lower.includes("date") || lower.includes("time")) {
    return "datetime";
  }
  if (lower.includes("blob")) {
    return "binary";
  }
  if (lower.includes("json")) {
    return "json";
  }
  return "string";
}

/**
 * Maps Rowpipe logical ColumnType to PostgreSQL DDL data type.
 */
export function rowpipeTypeToPostgres(type: ColumnType): string {
  switch (type) {
    case "integer":
      return "INTEGER";
    case "bigint":
      return "BIGINT";
    case "number":
      return "DOUBLE PRECISION";
    case "decimal":
      return "NUMERIC";
    case "boolean":
      return "BOOLEAN";
    case "date":
      return "DATE";
    case "datetime":
      return "TIMESTAMPTZ";
    case "json":
      return "JSONB";
    case "binary":
      return "BYTEA";
    default:
      return "TEXT";
  }
}

/**
 * Maps Rowpipe logical ColumnType to MySQL DDL data type.
 */
export function rowpipeTypeToMysql(type: ColumnType): string {
  switch (type) {
    case "integer":
      return "INT";
    case "bigint":
      return "BIGINT";
    case "number":
      return "DOUBLE";
    case "decimal":
      return "DECIMAL(65,30)";
    case "boolean":
      return "BOOLEAN";
    case "date":
      return "DATE";
    case "datetime":
      return "DATETIME(3)";
    case "json":
      return "JSON";
    case "binary":
      return "LONGBLOB";
    default:
      return "LONGTEXT";
  }
}

/**
 * Maps Rowpipe logical ColumnType to SQLite DDL data type.
 */
export function rowpipeTypeToSqlite(type: ColumnType): string {
  switch (type) {
    case "integer":
    case "bigint":
    case "boolean":
      return "INTEGER";
    case "number":
      return "REAL";
    case "decimal":
      return "TEXT";
    case "binary":
      return "BLOB";
    default:
      return "TEXT";
  }
}

/**
 * Generates dialect-specific CREATE TABLE SQL DDL.
 */
export function generateCreateTableSql(
  table: string,
  schema: Record<string, ColumnType> | { columns: Array<{ name: string; type: ColumnType; nullable?: boolean }> },
  dialect: DatabaseDialect,
  primaryKeys?: string[]
): string {
  const escapedTable = escapeQualifiedTable(table, dialect);
  const columnDefs: string[] = [];

  const typeMapper =
    dialect === "postgres"
      ? rowpipeTypeToPostgres
      : dialect === "mysql"
      ? rowpipeTypeToMysql
      : rowpipeTypeToSqlite;

  if ("columns" in schema && Array.isArray(schema.columns)) {
    for (const col of schema.columns) {
      const escapedCol = escapeIdentifier(col.name, dialect);
      const sqlType = typeMapper(col.type);
      const notNull = col.nullable === false ? " NOT NULL" : "";
      columnDefs.push(`  ${escapedCol} ${sqlType}${notNull}`);
    }
  } else {
    for (const [colName, colType] of Object.entries(schema)) {
      const escapedCol = escapeIdentifier(colName, dialect);
      const sqlType = typeMapper(colType as ColumnType);
      columnDefs.push(`  ${escapedCol} ${sqlType}`);
    }
  }

  if (primaryKeys && primaryKeys.length > 0) {
    const escapedPks = primaryKeys.map((pk) => escapeIdentifier(pk, dialect)).join(", ");
    columnDefs.push(`  PRIMARY KEY (${escapedPks})`);
  }

  return `CREATE TABLE IF NOT EXISTS ${escapedTable} (\n${columnDefs.join(",\n")}\n);`;
}
