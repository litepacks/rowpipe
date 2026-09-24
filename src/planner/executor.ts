import { reduceRows } from "../analytics/reduce.js";
import { createPipeline, Pipeline } from "../core/pipeline.js";
import type { DataStream, ReaderOptions, TabularReader } from "../core/types.js";
import { castColumns } from "../transforms/cast.js";
import { filterRows } from "../transforms/filter.js";
import { limitRows } from "../transforms/limit.js";
import { mapRows } from "../transforms/map.js";
import { offsetRows } from "../transforms/offset.js";
import { renameColumns } from "../transforms/rename.js";
import { selectColumns } from "../transforms/select.js";
import { sortRows } from "../transforms/sort/index.js";
import { tailRows } from "../transforms/tail.js";
import { topRows } from "../transforms/top.js";
import { uniqueRows } from "../transforms/unique.js";
import { windowRows } from "../transforms/window.js";
import { explodeRows } from "../transforms/explode.js";
import { flattenRows } from "../transforms/flatten.js";
import { PlannedPipeline } from "./types.js";

/**
 * Builds and executes a Pipeline from an optimized PlannedPipeline.
 */
export function executePlannedPipeline(
  source: DataStream | TabularReader,
  plan: PlannedPipeline,
  readerOptions?: ReaderOptions
): Pipeline {
  const mergedReaderOpts: ReaderOptions = {
    ...readerOptions,
    maxRows: plan.effectiveReadLimit,
  };

  const pipeline = createPipeline(source, mergedReaderOpts);

  for (const op of plan.operations) {
    switch (op.type) {
      case "filter":
        pipeline.pipe(filterRows(op.expression));
        break;

      case "select":
        pipeline.pipe(selectColumns(op.columns));
        break;

      case "rename":
        pipeline.pipe(renameColumns(op.specs));
        break;

      case "cast":
        pipeline.pipe(castColumns(op.specs, { onError: op.onError as any }));
        break;

      case "map":
        pipeline.pipe(mapRows(op.specs));
        break;

      case "sort":
        pipeline.pipe(
          sortRows({
            by: op.specs,
            nulls: op.nulls,
            ignoreCase: op.ignoreCase,
            natural: op.natural,
            memoryLimit: op.memoryLimit,
          })
        );
        break;

      case "top":
        pipeline.pipe(
          topRows({
            by: op.specs,
            count: op.count,
            order: op.order,
            smallest: op.smallest,
            nulls: op.nulls,
            ignoreCase: op.ignoreCase,
            natural: op.natural,
          })
        );
        break;

      case "limit":
        pipeline.pipe(limitRows(op.count));
        break;

      case "offset":
        pipeline.pipe(offsetRows(op.count));
        break;

      case "tail":
        pipeline.pipe(tailRows(op.count));
        break;

      case "unique":
        pipeline.pipe(
          uniqueRows({
            by: op.by,
            keep: op.keep,
            memoryLimit: op.memoryLimit,
          })
        );
        break;

      case "group":
        pipeline.pipe(
          reduceRows({
            by: op.by,
            aggregations: op.aggregations,
          })
        );
        break;

      case "window":
        pipeline.pipe(
          windowRows({
            specs: op.specs,
            by: op.by,
          })
        );
        break;

      case "explode":
        pipeline.pipe(
          explodeRows({
            column: op.column,
            delimiter: op.delimiter,
            trim: op.trim,
            dropEmpty: op.dropEmpty,
          })
        );
        break;

      case "flatten":
        pipeline.pipe(
          flattenRows({
            separator: op.separator,
            maxDepth: op.maxDepth,
            arrays: op.arrays,
          })
        );
        break;
    }
  }

  return pipeline;
}
