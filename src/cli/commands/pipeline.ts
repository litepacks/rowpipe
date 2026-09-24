import { buildGroupReduceSpecs } from "../../transforms/group.js";
import { parseSortSpecs, type SortKeySpec } from "../../transforms/sort/comparator.js";
import { executePlannedPipeline, optimizePipeline, PipelineOperation, PlannedPipeline } from "../../planner/index.js";
import { createReader, inferFormatFromPath as inferReaderFormat, isGlobPattern } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream, openWritableStream } from "../../utils/compression.js";
import { logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface UnifiedPipelineCliOptions {
  filter?: string | string[];
  select?: string | string[];
  rename?: string | string[];
  cast?: string | string[];
  map?: string | string[];
  window?: string | string[];
  partitionBy?: string | string[];
  by?: string | string[];
  sort?: string | string[];
  top?: string | number;
  limit?: string | number;
  offset?: string | number;
  tail?: string | number;
  unique?: string | boolean;
  groupBy?: string | string[];
  count?: boolean;
  sum?: string | string[];
  avg?: string | string[];
  min?: string | string[];
  max?: string | string[];
  first?: string | string[];
  last?: string | string[];
  agg?: string | string[];
  nulls?: "first" | "last";
  ignoreCase?: boolean;
  natural?: boolean;
  memoryLimit?: string;
  tempDir?: string;
  from?: string;
  to?: string;
  sheet?: string;
  delimiter?: string;
  json?: boolean;
  output?: string;
  batchSize?: string | number;
  addFilename?: boolean;
  fileCol?: string;
  gzip?: boolean;
  brotli?: boolean;
  zstd?: boolean;
  deflate?: boolean;
  compress?: string;
  compression?: string;
  toDb?: string;
  toTable?: string;
  table?: string;
  explode?: string | string[];
  explodeDelimiter?: string;
  flatten?: boolean | string;
  flattenSeparator?: string;
  transaction?: boolean;
  createTable?: boolean;
  upsert?: boolean;
  conflict?: string;
  truncate?: boolean;
  dryRun?: boolean;
  quiet?: boolean;
  noProgress?: boolean;
  onError?: "abort" | "skip" | "log" | "fail";
  badRowsLog?: string;
}

/**
 * Builds an AST of PipelineOperations from CLI flags.
 */
export function buildOperationsFromOptions(options: UnifiedPipelineCliOptions): PipelineOperation[] {
  const ops: PipelineOperation[] = [];

  // 0. Flatten
  if (options.flatten) {
    ops.push({
      type: "flatten",
      separator: typeof options.flatten === "string" ? options.flatten : (options.flattenSeparator ?? "."),
    });
  }

  // 0.1 Explode
  if (options.explode) {
    const cols = Array.isArray(options.explode) ? options.explode : [options.explode];
    for (const c of cols) {
      if (c && c.trim().length > 0) {
        ops.push({
          type: "explode",
          column: c.trim(),
          delimiter: options.explodeDelimiter ?? ",",
          trim: true,
          dropEmpty: true,
        });
      }
    }
  }

  // 1. Filter
  if (options.filter) {
    const filters = Array.isArray(options.filter) ? options.filter : [options.filter];
    for (const f of filters) {
      if (f && f.trim().length > 0) {
        ops.push({ type: "filter", expression: f });
      }
    }
  }

  // 2. Map
  if (options.map) {
    const maps = Array.isArray(options.map) ? options.map : [options.map];
    const mapSpecs: Record<string, string> = {};
    for (const m of maps) {
      const idx = m.indexOf("=");
      if (idx > 0) {
        mapSpecs[m.slice(0, idx).trim()] = m.slice(idx + 1).trim();
      }
    }
    if (Object.keys(mapSpecs).length > 0) {
      ops.push({ type: "map", specs: mapSpecs });
    }
  }

  // 3. Window
  if (options.window) {
    const windows = Array.isArray(options.window) ? options.window : [options.window];
    const windowSpecs: Record<string, string> = {};
    for (const w of windows) {
      const idx = w.indexOf("=");
      if (idx > 0) {
        windowSpecs[w.slice(0, idx).trim()] = w.slice(idx + 1).trim();
      }
    }
    if (Object.keys(windowSpecs).length > 0) {
      const byCols = options.partitionBy || options.by;
      const by = byCols
        ? (Array.isArray(byCols) ? byCols : byCols.split(",").map((s) => s.trim()).filter(Boolean))
        : undefined;
      ops.push({ type: "window", specs: windowSpecs, by });
    }
  }

  // 4. Rename
  if (options.rename) {
    const renames = Array.isArray(options.rename) ? options.rename : [options.rename];
    const renameSpecs: Record<string, string> = {};
    for (const r of renames) {
      const idx = r.indexOf("=");
      if (idx > 0) {
        renameSpecs[r.slice(0, idx).trim()] = r.slice(idx + 1).trim();
      }
    }
    if (Object.keys(renameSpecs).length > 0) {
      ops.push({ type: "rename", specs: renameSpecs });
    }
  }

  // 4. Cast
  if (options.cast) {
    const casts = Array.isArray(options.cast) ? options.cast : [options.cast];
    const castSpecs: Record<string, string> = {};
    for (const c of casts) {
      const idx = c.indexOf(":");
      if (idx > 0) {
        castSpecs[c.slice(0, idx).trim()] = c.slice(idx + 1).trim();
      }
    }
    if (Object.keys(castSpecs).length > 0) {
      ops.push({ type: "cast", specs: castSpecs });
    }
  }

  // 5. Select
  if (options.select) {
    const selects = Array.isArray(options.select) ? options.select : options.select.split(",");
    const cols = selects.map((s) => s.trim()).filter(Boolean);
    if (cols.length > 0) {
      ops.push({ type: "select", columns: cols });
    }
  }

  // 6. Unique
  if (options.unique !== undefined && options.unique !== false) {
    const by = typeof options.unique === "string" ? options.unique.split(",").map((s) => s.trim()).filter(Boolean) : undefined;
    ops.push({
      type: "unique",
      by,
      memoryLimit: options.memoryLimit,
    });
  }

  // 7. Group By / Aggregations
  if (options.groupBy || options.count || options.sum || options.avg || options.min || options.max || options.first || options.last || options.agg) {
    const by = options.groupBy ? (Array.isArray(options.groupBy) ? options.groupBy : options.groupBy.split(",").map((s) => s.trim()).filter(Boolean)) : undefined;
    const aggregations = buildGroupReduceSpecs({
      by,
      count: options.count,
      sum: options.sum,
      avg: options.avg,
      min: options.min,
      max: options.max,
      first: options.first,
      last: options.last,
      agg: options.agg,
    });
    ops.push({
      type: "group",
      by,
      aggregations,
      memoryLimit: options.memoryLimit,
    });
  }

  // 8. Sort
  if (options.sort) {
    const specs = parseSortSpecs(options.sort, {
      nulls: options.nulls,
      ignoreCase: options.ignoreCase,
      natural: options.natural,
    });
    ops.push({
      type: "sort",
      specs,
      nulls: options.nulls,
      ignoreCase: options.ignoreCase,
      natural: options.natural,
      memoryLimit: options.memoryLimit,
    });
  }

  // 9. Top
  if (options.top !== undefined) {
    const count = Number(options.top) || 10;
    const sortSpecs: SortKeySpec[] = options.sort
      ? parseSortSpecs(options.sort)
      : [{ column: "value", direction: "desc" }];
    ops.push({
      type: "top",
      specs: sortSpecs,
      count,
      order: "desc",
      smallest: false,
      nulls: options.nulls,
      ignoreCase: options.ignoreCase,
      natural: options.natural,
    });
  }

  // 10. Offset
  if (options.offset !== undefined) {
    const count = Number(options.offset) || 0;
    if (count > 0) {
      ops.push({ type: "offset", count });
    }
  }

  // 11. Limit
  if (options.limit !== undefined) {
    const count = Number(options.limit) || 0;
    ops.push({ type: "limit", count });
  }

  // 12. Tail
  if (options.tail !== undefined) {
    const count = Number(options.tail) || 10;
    ops.push({ type: "tail", count });
  }

  return ops;
}

export async function unifiedPipelineCommand(
  inputPath = "-",
  options: UnifiedPipelineCliOptions = {}
): Promise<void> {
  const operations = buildOperationsFromOptions(options);
  const plan = optimizePipeline(operations);

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) {
    toFormat = fromFormat === "xlsx" ? "csv" : fromFormat;
  }

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    maxRows: plan.effectiveReadLimit,
    filePath: inputPath,
    table: options.table,
    addFilename: options.addFilename,
    fileCol: options.fileCol,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const conflictCols = options.conflict
    ? options.conflict.split(",").map((s) => s.trim()).filter(Boolean)
    : undefined;

  const targetWriter = options.toDb
    ? createWriter(options.toDb, {
        table: options.toTable || options.table || "output",
        transaction: options.transaction,
        createTable: options.createTable,
        upsert: options.upsert,
        conflictColumns: conflictCols,
        truncate: options.truncate,
      })
    : createWriter(options.output || "-", {
        format: toFormat,
        delimiter: options.delimiter,
        ...options,
      });

  const pipeline = executePlannedPipeline(reader, plan, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  try {
    await pipeline.to(targetWriter);
  } finally {
    if (reader.close) await reader.close();
    if (targetWriter.close) await targetWriter.close();
  }

  progress.done();
  logMemoryDebug();
}
