import type { PipelineOperation } from "../planner/types.js";
import { escapeIdentifier, escapeQualifiedTable } from "./mapping.js";
import type { DatabaseDialect } from "./types.js";

export interface PushdownAnalysis {
  generatedQuery: string;
  remainingOperations: PipelineOperation[];
  pushedClauses: {
    select?: string[];
    where?: string;
    orderBy?: string[];
    limit?: number;
    offset?: number;
    count?: boolean;
  };
  pushedSummary: string[];
}

/**
 * Attempts to safely translate a simple Rowpipe filter expression to SQL WHERE clause.
 * Returns undefined if expression contains unsupported operators, pipes, or dialect divergences.
 */
export function tryTranslateFilterToSql(
  expression: string,
  dialect: DatabaseDialect
): string | undefined {
  const trimmed = expression.trim();
  if (!trimmed) return undefined;

  // If expression uses pipe transformations (e.g. "email | lower"), keep it local
  if (trimmed.includes("|")) {
    return undefined;
  }

  // Check for safe simple comparison patterns:
  // identifier (==|!=|>=|<=|>|<) (string|number|boolean|null)
  // connected by && / and or || / or
  const orParts = trimmed.split(/\s*\|\|\s*|\s+or\s+/i);
  const sqlOrClauses: string[] = [];

  for (const orPart of orParts) {
    const andParts = orPart.split(/\s*&&\s*|\s+and\s+/i);
    const sqlAndClauses: string[] = [];

    for (const part of andParts) {
      const match = part.match(
        /^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*(==|!=|>=|<=|>|<)\s*(.+?)\s*$/
      );
      if (!match) {
        return undefined; // Too complex or unsafe for automatic pushdown
      }

      const rawIdent = match[1]!;
      const rawOp = match[2]!;
      const rawVal = match[3]!.trim();

      const escapedCol = escapeIdentifier(rawIdent, dialect);

      // Null check
      if (rawVal === "null" || rawVal === "undefined") {
        if (rawOp === "==" || rawOp === "=") {
          sqlAndClauses.push(`${escapedCol} IS NULL`);
        } else if (rawOp === "!=") {
          sqlAndClauses.push(`${escapedCol} IS NOT NULL`);
        } else {
          return undefined;
        }
        continue;
      }

      // Boolean
      if (rawVal === "true" || rawVal === "false") {
        const boolVal = rawVal === "true";
        if (dialect === "postgres") {
          sqlAndClauses.push(`${escapedCol} ${rawOp === "==" ? "=" : "<>"} ${boolVal ? "TRUE" : "FALSE"}`);
        } else if (dialect === "mysql" || dialect === "sqlite") {
          sqlAndClauses.push(`${escapedCol} ${rawOp === "==" ? "=" : "<>"} ${boolVal ? "1" : "0"}`);
        }
        continue;
      }

      // Number
      if (/^-?\d+(\.\d+)?$/.test(rawVal)) {
        const op = rawOp === "==" ? "=" : rawOp === "!=" ? "<>" : rawOp;
        sqlAndClauses.push(`${escapedCol} ${op} ${rawVal}`);
        continue;
      }

      // String literal
      if ((rawVal.startsWith('"') && rawVal.endsWith('"')) || (rawVal.startsWith("'") && rawVal.endsWith("'"))) {
        const unquoted = rawVal.slice(1, -1);
        const escapedStr = unquoted.replace(/'/g, "''");
        const op = rawOp === "==" ? "=" : rawOp === "!=" ? "<>" : rawOp;
        sqlAndClauses.push(`${escapedCol} ${op} '${escapedStr}'`);
        continue;
      }

      // Identifier vs identifier or complex expression
      return undefined;
    }

    sqlOrClauses.push(sqlAndClauses.length > 1 ? `(${sqlAndClauses.join(" AND ")})` : sqlAndClauses[0]!);
  }

  return sqlOrClauses.length > 1 ? `(${sqlOrClauses.join(" OR ")})` : sqlOrClauses[0];
}

/**
 * Analyzes pipeline operations for a table query and extracts safe pushdown clauses.
 */
export function analyzeDatabasePushdown(
  table: string,
  dialect: DatabaseDialect,
  operations: PipelineOperation[],
  explicitWhere?: string
): PushdownAnalysis {
  let selectCols: string[] | undefined;
  let whereSql = explicitWhere?.trim() || undefined;
  let orderByClauses: string[] | undefined;
  let limitVal: number | undefined;
  let offsetVal: number | undefined;

  const remaining: PipelineOperation[] = [];
  const pushedSummary: string[] = [];

  for (let i = 0; i < operations.length; i++) {
    const op = operations[i]!;

    if (op.type === "select" && !selectCols) {
      selectCols = op.columns;
      pushedSummary.push(`SELECT ${op.columns.join(", ")}`);
      continue;
    }

    if (op.type === "filter") {
      const sqlClause = tryTranslateFilterToSql(op.expression, dialect);
      if (sqlClause) {
        whereSql = whereSql ? `${whereSql} AND ${sqlClause}` : sqlClause;
        pushedSummary.push(`WHERE ${sqlClause}`);
        continue;
      }
    }

    if (op.type === "sort" && !orderByClauses) {
      orderByClauses = op.specs.map((s) => {
        const col = escapeIdentifier(s.column, dialect);
        const dir = (s.direction || "asc").toUpperCase();
        return `${col} ${dir}`;
      });
      pushedSummary.push(`ORDER BY ${orderByClauses.join(", ")}`);
      continue;
    }

    if (op.type === "top" && !orderByClauses && limitVal === undefined) {
      orderByClauses = op.specs.map((s) => {
        const col = escapeIdentifier(s.column, dialect);
        const dir = (s.direction || (op.order === "asc" ? "ASC" : "DESC")).toUpperCase();
        return `${col} ${dir}`;
      });
      limitVal = op.count;
      pushedSummary.push(`ORDER BY ${orderByClauses.join(", ")} LIMIT ${op.count}`);
      continue;
    }

    if (op.type === "offset" && offsetVal === undefined) {
      offsetVal = op.count;
      pushedSummary.push(`OFFSET ${op.count}`);
      continue;
    }

    if (op.type === "limit" && limitVal === undefined) {
      limitVal = op.count;
      pushedSummary.push(`LIMIT ${op.count}`);
      continue;
    }

    // Operation could not be pushed down safely; retain for local streaming execution
    remaining.push(op);
  }

  // Build final SQL string
  const escapedTable = escapeQualifiedTable(table, dialect);
  const selectClause = selectCols && selectCols.length > 0
    ? selectCols.map((c) => escapeIdentifier(c, dialect)).join(", ")
    : "*";

  let sql = `SELECT ${selectClause} FROM ${escapedTable}`;

  if (whereSql) {
    sql += ` WHERE ${whereSql}`;
  }

  if (orderByClauses && orderByClauses.length > 0) {
    sql += ` ORDER BY ${orderByClauses.join(", ")}`;
  }

  if (limitVal !== undefined) {
    sql += ` LIMIT ${limitVal}`;
  }

  if (offsetVal !== undefined) {
    sql += ` OFFSET ${offsetVal}`;
  }

  return {
    generatedQuery: sql,
    remainingOperations: remaining,
    pushedClauses: {
      select: selectCols,
      where: whereSql,
      orderBy: orderByClauses,
      limit: limitVal,
      offset: offsetVal,
    },
    pushedSummary,
  };
}
