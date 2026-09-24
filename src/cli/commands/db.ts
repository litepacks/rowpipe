import { DatabaseReader } from "../../db/source.js";
import { DatabaseWriter } from "../../db/sink.js";
import { createDatabaseAdapter } from "../../db/adapters/index.js";
import { isDatabaseUrl, parseDatabaseUrl, sanitizeConnectionString } from "../../db/url.js";
import { analyzeDatabasePushdown } from "../../db/pushdown.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openWritableStream } from "../../utils/compression.js";
import { formatTable, logMemoryDebug, safeJsonStringify } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";
import { executePlannedPipeline, optimizePipeline } from "../../planner/index.js";
import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";
import { InvalidArgumentError } from "../../core/errors.js";

export interface DbCommandOptions extends UnifiedPipelineCliOptions {
  table?: string;
  query?: string;
  params?: string | string[];
  param?: string | string[];
  where?: string;
  tables?: boolean;
  schema?: string | boolean;
  toDb?: string;
  toTable?: string;
  transaction?: boolean;
  createTable?: boolean;
  upsert?: boolean;
  conflict?: string;
  truncate?: boolean;
  dryRun?: boolean;
}

/**
 * Main handler for `rowpipe db <url> [options]`.
 */
export async function dbCommand(
  dbUrl: string,
  options: DbCommandOptions = {}
): Promise<void> {
  if (!dbUrl || dbUrl === "-") {
    throw new InvalidArgumentError("Database connection URL is required (e.g. postgres://..., mysql://..., sqlite://...)");
  }

  // Handle table list introspection
  if (options.tables) {
    await dbTablesCommand(dbUrl, options);
    return;
  }

  // Handle schema inspection
  if (options.schema) {
    const table = typeof options.schema === "string" ? options.schema : options.table;
    if (!table) {
      throw new InvalidArgumentError("Specify table to inspect schema (e.g. rowpipe db <url> --schema <table_name>)");
    }
    await dbSchemaCommand(dbUrl, table, options);
    return;
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  // Parse query parameters
  let queryParams: unknown[] = [];
  if (options.params) {
    const raw = Array.isArray(options.params) ? options.params.join("") : options.params;
    try {
      queryParams = JSON.parse(raw);
    } catch {
      queryParams = [raw];
    }
  } else if (options.param) {
    queryParams = Array.isArray(options.param) ? options.param : [options.param];
  }

  const rawOperations = buildOperationsFromOptions(options);
  const parsedConfig = parseDatabaseUrl(dbUrl);

  let finalQuery: string | undefined = options.query;
  let remainingOps = rawOperations;

  // Pushdown analysis when using --table
  if (options.table && !options.query) {
    const analysis = analyzeDatabasePushdown(
      options.table,
      parsedConfig.dialect,
      rawOperations,
      options.where
    );
    finalQuery = analysis.generatedQuery;
    remainingOps = analysis.remainingOperations;
  }

  const optimizedPlan = optimizePipeline(remainingOps);

  const reader = new DatabaseReader(parsedConfig, {
    table: options.table,
    query: finalQuery,
    params: queryParams,
    batchSize: effectiveBatchSize,
  });

  // Check if output is another database sink
  if (options.toDb) {
    const targetTable = options.toTable || options.table;
    if (!targetTable) {
      throw new InvalidArgumentError("Specify target table when writing to database (e.g. --to-table <name>)");
    }

    const conflictCols = options.conflict
      ? options.conflict.split(",").map((s) => s.trim()).filter(Boolean)
      : undefined;

    const writer = new DatabaseWriter(options.toDb, {
      table: targetTable,
      transaction: options.transaction,
      createTable: options.createTable,
      upsert: options.upsert,
      conflictColumns: conflictCols,
      truncate: options.truncate,
      dryRun: options.dryRun,
    });

    const pipeline = executePlannedPipeline(reader, optimizedPlan, { batchSize: effectiveBatchSize });
    pipeline.onProgress((info) => progress.update(info));

    try {
      await pipeline.to(writer);
    } finally {
      await reader.close();
      await writer.close();
    }

    progress.done();
    logMemoryDebug();
    return;
  }

  // Standard File / Stdout Writer
  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) {
    toFormat = "csv";
  }

  const effectiveOutput = options.output || "-";
  const outputStream = openWritableStream(effectiveOutput);
  const writer = createWriter(outputStream, {
    format: toFormat,
    delimiter: options.delimiter,
  });

  const pipeline = executePlannedPipeline(reader, optimizedPlan, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  try {
    await pipeline.to(writer);
  } finally {
    await reader.close();
  }

  progress.done();
  logMemoryDebug();
}

/**
 * List all tables in the specified database.
 */
export async function dbTablesCommand(
  dbUrl: string,
  options: { json?: boolean } = {}
): Promise<void> {
  const adapter = createDatabaseAdapter(dbUrl);
  try {
    const tables = await adapter.getTables();

    if (options.json) {
      process.stdout.write(safeJsonStringify(tables, 2) + "\n");
      return;
    }

    if (tables.length === 0) {
      process.stdout.write("(no tables found)\n");
      return;
    }

    process.stdout.write(tables.join("\n") + "\n");
  } finally {
    await adapter.close();
  }
}

/**
 * Inspect the schema of a specific table in the database.
 */
export async function dbSchemaCommand(
  dbUrl: string,
  table: string,
  options: { json?: boolean } = {}
): Promise<void> {
  const adapter = createDatabaseAdapter(dbUrl);
  try {
    const schema = await adapter.getTableSchema(table);

    if (options.json) {
      process.stdout.write(safeJsonStringify(schema, 2) + "\n");
      return;
    }

    const headers = ["COLUMN", "DATABASE TYPE", "ROWPIPE TYPE", "NULLABLE", "PRIMARY KEY"];
    const rows = schema.columns.map((c) => [
      c.name,
      c.dbType,
      c.rowpipeType,
      c.nullable ? "YES" : "NO",
      c.isPrimaryKey ? "YES" : "NO",
    ]);

    process.stdout.write(`\nTable: ${table} (${adapter.dialect})\n\n`);
    process.stdout.write(formatTable(headers, rows) + "\n");
  } finally {
    await adapter.close();
  }
}
