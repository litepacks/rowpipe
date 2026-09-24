import { stat } from "node:fs/promises";
import { z } from "mcponce";
import { SchemaInferenceAggregator } from "../analytics/schema-inference.js";
import { DatasetStatsAggregator } from "../analytics/stats.js";
import { DatasetProfiler, formatProfileMarkdown, formatProfileTerminal } from "../analytics/profiler.js";
import { computeDiff, diffRows } from "../diff/engine.js";
import { convertCommand } from "../cli/commands/convert.js";
import { buildOperationsFromOptions } from "../cli/commands/pipeline.js";
import { optimizePipeline, executePlannedPipeline } from "../planner/index.js";
import { createPipeline } from "../core/pipeline.js";
import { createReader, inferFormatFromPath } from "../readers/index.js";
import { sampleRows } from "../transforms/sample.js";
import { formatBytes, formatDecimal, formatNumber, formatTable } from "../utils/formatting.js";
import type { Row } from "../core/types.js";

/**
 * MCP Tools for rowpipe tabular data operations.
 */

// 1. rowpipe_inspect
export const inspectToolDefinition = {
  name: "rowpipe_inspect",
  description:
    "Inspect a tabular data file (CSV, TSV, JSON, JSONL, Parquet, XLSX) to retrieve file size, row count, column count, types, null percentages, approximate distinct percentages, or sheet summaries.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    sheet: z.string().optional().describe("For XLSX workbooks, the specific sheet name to inspect"),
    delimiter: z.string().optional().describe("Custom delimiter character for delimited text files (e.g. ',', ';', '\\t')"),
  }),
  handler: async (args: { filePath: string; sheet?: string; delimiter?: string }) => {
    let fileSize: number | undefined;
    try {
      const fileStat = await stat(args.filePath);
      fileSize = fileStat.size;
    } catch (err: any) {
      throw new Error(`Cannot access file "${args.filePath}": ${err.message}`);
    }

    const format = inferFormatFromPath(args.filePath) || "csv";
    const reader = createReader(args.filePath, {
      format,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
    });

    try {
      // XLSX workbook inspection when no specific sheet requested
      if (format === "xlsx" && !args.sheet && reader.inspect) {
        const meta = await reader.inspect();
        return {
          file: args.filePath,
          format: "XLSX",
          sizeBytes: fileSize,
          sizeFormatted: fileSize !== undefined ? formatBytes(fileSize) : undefined,
          sheetsCount: meta.sheets?.length ?? 0,
          sheets: meta.sheets?.map((s) => ({
            name: s.name,
            rows: s.rowCount,
            columnsCount: s.columnCount,
            columnsPreview: s.columns?.slice(0, 10),
          })),
        };
      }

      const schemaAgg = new SchemaInferenceAggregator();
      const statsAgg = new DatasetStatsAggregator();
      const pipeline = createPipeline(reader);

      for await (const row of pipeline.rows()) {
        schemaAgg.add(row);
        statsAgg.add(row);
      }

      const schemaRes = schemaAgg.result();
      const statsRes = statsAgg.result();
      const totalRows = schemaRes.totalRowsScanned;

      const columns = schemaRes.columns.map((col) => {
        const nullPct = col.sampleCount > 0 ? (col.nullCount / col.sampleCount) * 100 : 0;
        const colStats = statsRes.columns[col.name];
        const approxDistinct = colStats?.numeric?.approxDistinct ?? colStats?.string?.approxDistinct ?? 0;
        const uniquePct = totalRows > 0 ? Math.min(100, (approxDistinct / totalRows) * 100) : 0;

        return {
          name: col.name,
          type: col.type,
          nullPercentage: Math.round(nullPct * 10) / 10,
          approxUniquePercentage: Math.round(uniquePct * 10) / 10,
          semantic: col.semantic,
        };
      });

      return {
        file: args.filePath,
        format: format.toUpperCase(),
        sheet: args.sheet,
        sizeBytes: fileSize,
        sizeFormatted: fileSize !== undefined ? formatBytes(fileSize) : undefined,
        rows: totalRows,
        columnsCount: columns.length,
        columns,
      };
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

// 2. rowpipe_query
export const queryToolDefinition = {
  name: "rowpipe_query",
  description:
    "Stream-query and transform tabular data with filter expressions, column selection, renaming, mapped expressions, casting, sorting, and pagination in bounded O(1) memory.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    filter: z.union([z.string(), z.array(z.string())]).optional().describe("Filter expression(s), e.g. 'age > 30' or 'country == \"TR\"'"),
    select: z.union([z.string(), z.array(z.string())]).optional().describe("Column(s) to select, e.g. 'id,name,age' or ['id', 'name']"),
    rename: z.union([z.string(), z.array(z.string())]).optional().describe("Column rename expressions, e.g. 'old_name=new_name'"),
    map: z.union([z.string(), z.array(z.string())]).optional().describe("Computed column expressions, e.g. 'total=price * qty'"),
    cast: z.union([z.string(), z.array(z.string())]).optional().describe("Column cast expressions, e.g. 'age:int' or 'price:float'"),
    sort: z.union([z.string(), z.array(z.string())]).optional().describe("Sort keys, e.g. 'age:desc' or 'name:asc'"),
    limit: z.number().optional().describe("Maximum rows to return (default: 100, capped at 1000)"),
    offset: z.number().optional().describe("Number of initial rows to skip"),
    sheet: z.string().optional().describe("Sheet name for XLSX workbooks"),
    delimiter: z.string().optional().describe("Custom delimiter character"),
    format: z.enum(["json", "markdown", "table"]).optional().describe("Result format: 'json' (default), 'markdown', or 'table'"),
  }),
  handler: async (args: {
    filePath: string;
    filter?: string | string[];
    select?: string | string[];
    rename?: string | string[];
    map?: string | string[];
    cast?: string | string[];
    sort?: string | string[];
    limit?: number;
    offset?: number;
    sheet?: string;
    delimiter?: string;
    format?: "json" | "markdown" | "table";
  }) => {
    const effectiveLimit = Math.min(Math.max(1, args.limit ?? 100), 1000);
    const operations = buildOperationsFromOptions({
      filter: args.filter,
      select: args.select,
      rename: args.rename,
      map: args.map,
      cast: args.cast,
      sort: args.sort,
      offset: args.offset,
      limit: effectiveLimit,
    });

    const plan = optimizePipeline(operations);
    const fromFormat = inferFormatFromPath(args.filePath) || "csv";

    const reader = createReader(args.filePath, {
      format: fromFormat,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
      maxRows: plan.effectiveReadLimit,
    });

    try {
      const pipeline = executePlannedPipeline(reader, plan, { batchSize: 1000 });
      const rows: Row[] = [];

      for await (const row of pipeline.rows()) {
        rows.push(row);
        if (rows.length >= effectiveLimit) break;
      }

      if (args.format === "markdown" || args.format === "table") {
        if (rows.length === 0) return "(No matching rows)";
        const headers = Object.keys(rows[0]!);
        const tableRows = rows.map((r) => headers.map((h) => (r[h] === null || r[h] === undefined ? "" : String(r[h]))));

        if (args.format === "markdown") {
          const headerLine = `| ${headers.join(" | ")} |`;
          const separatorLine = `| ${headers.map(() => "---").join(" | ")} |`;
          const rowLines = tableRows.map((tr) => `| ${tr.join(" | ")} |`);
          return [headerLine, separatorLine, ...rowLines].join("\n");
        }

        return formatTable(headers, tableRows);
      }

      return {
        totalReturned: rows.length,
        columns: rows.length > 0 ? Object.keys(rows[0]!) : [],
        rows,
      };
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

// 3. rowpipe_schema
export const schemaToolDefinition = {
  name: "rowpipe_schema",
  description:
    "Infer accurate schema, SQL data types, null rates, and semantic types (email, url, uuid, date, etc.) for any tabular dataset.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    sample: z.number().optional().describe("Maximum rows to scan for schema inference (default: 10000)"),
    sheet: z.string().optional().describe("For XLSX workbooks, sheet name"),
    delimiter: z.string().optional().describe("Custom delimiter"),
  }),
  handler: async (args: { filePath: string; sample?: number; sheet?: string; delimiter?: string }) => {
    const sampleLimit = Math.max(1, args.sample ?? 10000);
    const format = inferFormatFromPath(args.filePath) || "csv";
    const reader = createReader(args.filePath, {
      format,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
    });

    try {
      const schemaAgg = new SchemaInferenceAggregator({ sample: sampleLimit });
      const pipeline = createPipeline(reader, { maxRows: sampleLimit });

      for await (const row of pipeline.rows()) {
        schemaAgg.add(row);
      }

      return schemaAgg.result();
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

// 4. rowpipe_stats
export const statsToolDefinition = {
  name: "rowpipe_stats",
  description:
    "Calculate summary statistics (min, max, mean, sum, quantiles, approx distinct, null count) for numeric and string columns.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    column: z.string().optional().describe("Specific column name to compute stats for (default: all columns)"),
    sheet: z.string().optional().describe("For XLSX workbooks, sheet name"),
    delimiter: z.string().optional().describe("Custom delimiter"),
  }),
  handler: async (args: { filePath: string; column?: string; sheet?: string; delimiter?: string }) => {
    const format = inferFormatFromPath(args.filePath) || "csv";
    const reader = createReader(args.filePath, {
      format,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
    });

    try {
      const statsAgg = new DatasetStatsAggregator({ column: args.column });
      const pipeline = createPipeline(reader);

      for await (const row of pipeline.rows()) {
        statsAgg.add(row);
      }

      return statsAgg.result();
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

// 5. rowpipe_sample
export const sampleToolDefinition = {
  name: "rowpipe_sample",
  description:
    "Perform reservoir sampling on a tabular dataset with bounded O(k) memory and optional deterministic random seed.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    size: z.number().optional().describe("Number of sample rows to retrieve (default: 10)"),
    seed: z.number().optional().describe("Deterministic random seed integer"),
    sheet: z.string().optional().describe("For XLSX workbooks, sheet name"),
    delimiter: z.string().optional().describe("Custom delimiter"),
    format: z.enum(["json", "markdown", "table"]).optional().describe("Output representation: 'json' (default), 'markdown', or 'table'"),
  }),
  handler: async (args: {
    filePath: string;
    size?: number;
    seed?: number;
    sheet?: string;
    delimiter?: string;
    format?: "json" | "markdown" | "table";
  }) => {
    const sampleSize = Math.max(1, Math.min(args.size ?? 10, 500));
    const format = inferFormatFromPath(args.filePath) || "csv";
    const reader = createReader(args.filePath, {
      format,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
    });

    try {
      const pipeline = createPipeline(reader).pipe(
        sampleRows({ rows: sampleSize, seed: args.seed })
      );

      const rows: Row[] = [];
      for await (const row of pipeline.rows()) {
        rows.push(row);
      }

      if (args.format === "markdown" || args.format === "table") {
        if (rows.length === 0) return "(No rows sampled)";
        const headers = Object.keys(rows[0]!);
        const tableRows = rows.map((r) => headers.map((h) => (r[h] === null || r[h] === undefined ? "" : String(r[h]))));

        if (args.format === "markdown") {
          const headerLine = `| ${headers.join(" | ")} |`;
          const separatorLine = `| ${headers.map(() => "---").join(" | ")} |`;
          const rowLines = tableRows.map((tr) => `| ${tr.join(" | ")} |`);
          return [headerLine, separatorLine, ...rowLines].join("\n");
        }

        return formatTable(headers, tableRows);
      }

      return {
        sampleSize: rows.length,
        rows,
      };
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

// 6. rowpipe_convert
export const convertToolDefinition = {
  name: "rowpipe_convert",
  description:
    "Convert tabular datasets between formats (CSV, TSV, JSON, JSONL, Parquet, XLSX, Markdown) with high throughput streaming.",
  inputSchema: z.object({
    inputPath: z.string().describe("Source file path"),
    outputPath: z.string().describe("Target file path (extension determines format: .csv, .json, .parquet, .xlsx, .md)"),
    sheet: z.string().optional().describe("For XLSX input/output, sheet name"),
    delimiter: z.string().optional().describe("Custom delimiter for delimited formats"),
  }),
  handler: async (args: { inputPath: string; outputPath: string; sheet?: string; delimiter?: string }) => {
    await convertCommand(args.inputPath, args.outputPath, {
      sheet: args.sheet,
      delimiter: args.delimiter,
      quiet: true,
      noProgress: true,
    });

    const targetStat = await stat(args.outputPath);
    return {
      success: true,
      input: args.inputPath,
      output: args.outputPath,
      outputSizeBytes: targetStat.size,
      outputSizeFormatted: formatBytes(targetStat.size),
    };
  },
};

// 7. rowpipe_diff
export const diffToolDefinition = {
  name: "rowpipe_diff",
  description:
    "Compare two tabular datasets with key-based row-level diffing, identifying added, removed, changed, and unchanged rows and column differences.",
  inputSchema: z.object({
    leftPath: z.string().describe("Baseline dataset path"),
    rightPath: z.string().describe("Comparison dataset path"),
    key: z.string().describe("Key column(s), comma-separated (e.g. 'id' or 'id,date')"),
    columns: z.string().optional().describe("Columns to compare (comma-separated; default: all)"),
    ignore: z.string().optional().describe("Columns to ignore (comma-separated)"),
    sheet: z.string().optional().describe("Sheet name for XLSX workbooks"),
    limit: z.number().optional().describe("Maximum sample diff events to include in output (default: 20)"),
  }),
  handler: async (args: {
    leftPath: string;
    rightPath: string;
    key: string;
    columns?: string;
    ignore?: string;
    sheet?: string;
    limit?: number;
  }) => {
    const keys = args.key.split(",").map((k) => k.trim()).filter(Boolean);
    const compareColumns = args.columns ? args.columns.split(",").map((c) => c.trim()).filter(Boolean) : undefined;
    const ignoreColumns = args.ignore ? args.ignore.split(",").map((c) => c.trim()).filter(Boolean) : undefined;

    const summary = await computeDiff({
      left: args.leftPath,
      right: args.rightPath,
      keys,
      columns: compareColumns,
      ignore: ignoreColumns,
      leftOptions: { sheet: args.sheet, filePath: args.leftPath },
      rightOptions: { sheet: args.sheet, filePath: args.rightPath },
    });

    // Collect sample events if requested
    const sampleLimit = Math.max(0, Math.min(args.limit ?? 20, 100));
    const sampleEvents: any[] = [];

    if (sampleLimit > 0) {
      for await (const event of diffRows({
        left: args.leftPath,
        right: args.rightPath,
        keys,
        columns: compareColumns,
        ignore: ignoreColumns,
        leftOptions: { sheet: args.sheet, filePath: args.leftPath },
        rightOptions: { sheet: args.sheet, filePath: args.rightPath },
      })) {
        if (event.type !== "unchanged") {
          sampleEvents.push(event);
          if (sampleEvents.length >= sampleLimit) break;
        }
      }
    }

    return {
      left: args.leftPath,
      right: args.rightPath,
      keys,
      rows: summary.rows,
      changedColumns: summary.columns,
      schemaDiff: summary.schema,
      sampleEvents,
    };
  },
};

// 8. rowpipe_profile
export const profileToolDefinition = {
  name: "rowpipe_profile",
  description:
    "Comprehensive data profile with type inference, null percentages, distinct counts, distributions, and quality warnings.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    sample: z.number().optional().describe("Maximum rows to profile (default: all)"),
    sheet: z.string().optional().describe("Sheet name for XLSX workbooks"),
    delimiter: z.string().optional().describe("Custom delimiter"),
    format: z.enum(["json", "markdown", "summary"]).optional().describe("Output representation: 'json' (default), 'markdown', or 'summary'"),
  }),
  handler: async (args: {
    filePath: string;
    sample?: number;
    sheet?: string;
    delimiter?: string;
    format?: "json" | "markdown" | "summary";
  }) => {
    const format = inferFormatFromPath(args.filePath) || "csv";
    const sampleLimit = args.sample !== undefined ? Math.max(1, args.sample) : undefined;

    const reader = createReader(args.filePath, {
      format,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
      maxRows: sampleLimit,
    });

    try {
      const profiler = new DatasetProfiler();
      const pipeline = createPipeline(reader, { maxRows: sampleLimit });
      const result = await pipeline.reduce(profiler);

      if (args.format === "markdown") {
        return formatProfileMarkdown(result);
      }
      if (args.format === "summary") {
        return formatProfileTerminal(result);
      }

      return result;
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

// 9. rowpipe_table
export const tableToolDefinition = {
  name: "rowpipe_table",
  description: "Render a clean, aligned tabular preview of a dataset with optional filtering and column selection.",
  inputSchema: z.object({
    filePath: z.string().describe("Path to the tabular data file"),
    limit: z.number().optional().describe("Maximum rows to preview (default: 20)"),
    filter: z.string().optional().describe("Filter expression (e.g. 'age >= 21')"),
    select: z.string().optional().describe("Comma-separated list of columns to display"),
    sheet: z.string().optional().describe("Sheet name for XLSX workbooks"),
    delimiter: z.string().optional().describe("Custom delimiter"),
  }),
  handler: async (args: {
    filePath: string;
    limit?: number;
    filter?: string;
    select?: string;
    sheet?: string;
    delimiter?: string;
  }) => {
    const effectiveLimit = Math.max(1, Math.min(args.limit ?? 20, 200));
    const operations = buildOperationsFromOptions({
      filter: args.filter,
      select: args.select,
      limit: effectiveLimit,
    });

    const plan = optimizePipeline(operations);
    const format = inferFormatFromPath(args.filePath) || "csv";
    const reader = createReader(args.filePath, {
      format,
      sheet: args.sheet,
      delimiter: args.delimiter,
      filePath: args.filePath,
      maxRows: plan.effectiveReadLimit,
    });

    try {
      const pipeline = executePlannedPipeline(reader, plan, { batchSize: 500 });
      const rows: Row[] = [];

      for await (const row of pipeline.rows()) {
        rows.push(row);
        if (rows.length >= effectiveLimit) break;
      }

      if (rows.length === 0) {
        return "(No rows to display)";
      }

      const headers = Object.keys(rows[0]!);
      const tableRows = rows.map((r) => headers.map((h) => (r[h] === null || r[h] === undefined ? "" : String(r[h]))));
      return formatTable(headers, tableRows);
    } finally {
      if (reader.close) await reader.close();
    }
  },
};

export const allRowpipeTools = [
  inspectToolDefinition,
  queryToolDefinition,
  schemaToolDefinition,
  statsToolDefinition,
  sampleToolDefinition,
  convertToolDefinition,
  diffToolDefinition,
  profileToolDefinition,
  tableToolDefinition,
];
