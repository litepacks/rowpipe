// Core
export * from "./core/types.js";
export * from "./core/errors.js";
export * from "./core/error-handler.js";
export * from "./core/batch.js";
export * from "./core/pipeline.js";

// Readers
export {
  CSVReader,
  type CSVReaderOptions,
  JSONReader,
  type JSONReaderOptions,
  JSONLReader,
  type JSONLReaderOptions,
  ParquetReader,
  type ParquetReaderOptions,
  XLSXReader,
  type XLSXReaderOptions,
  FileSystemReader,
  MultiFileReader,
  type MultiFileReaderOptions,
  createReader,
  inferFormatFromPath as inferReaderFormatFromPath,
} from "./readers/index.js";

// Filesystem Subsystem
export * from "./files/types.js";
export * from "./files/glob.js";
export * from "./files/mime.js";
export * from "./files/hash.js";
export * from "./files/reader.js";


// Writers
export {
  CSVWriter,
  type CSVWriterOptions,
  JSONWriter,
  JSONLWriter,
  MarkdownWriter,
  type MarkdownWriterOptions,
  ParquetWriter,
  type ParquetWriterOptions,
  XLSXWriter,
  type XLSXWriterOptions,
  TableWriter,
  type TableWriterOptions,
  formatTable,
  createWriter,
  inferFormatFromPath as inferWriterFormatFromPath,
} from "./writers/index.js";

// Transforms
export * from "./transforms/select.js";
export * from "./transforms/rename.js";
export * from "./transforms/cast.js";
export * from "./transforms/sample.js";
export * from "./transforms/filter.js";
export * from "./transforms/expression.js";
export * from "./transforms/map.js";
export * from "./transforms/limit.js";
export * from "./transforms/offset.js";
export * from "./transforms/tail.js";
export * from "./transforms/top.js";
export * from "./transforms/sort/index.js";
export * from "./transforms/unique.js";
export * from "./transforms/group.js";
export * from "./transforms/count.js";
export * from "./transforms/join/index.js";
export * from "./transforms/window.js";
export * from "./transforms/clean.js";
export * from "./transforms/explode.js";
export * from "./transforms/flatten.js";
export * from "./transforms/partition.js";
export * from "./transforms/split.js";
export * from "./transforms/timeseries.js";
export * from "./transforms/pivot.js";
export * from "./transforms/unpivot.js";
export * from "./transforms/fuzzy-join.js";
export * from "./transforms/concat.js";
export * from "./cli/commands/explode.js";
export * from "./cli/commands/flatten.js";
export * from "./cli/commands/freq.js";
export * from "./cli/commands/plot.js";
export * from "./cli/commands/table.js";
export * from "./cli/commands/completion.js";
export * from "./cli/commands/partition.js";
export * from "./cli/commands/split.js";
export * from "./cli/commands/timeseries.js";
export * from "./cli/commands/corr.js";
export * from "./cli/commands/quantiles.js";
export * from "./cli/commands/outliers.js";
export * from "./cli/commands/crosstab.js";
export * from "./cli/commands/regression.js";
export * from "./cli/commands/rfm.js";
export * from "./cli/commands/cohort.js";
export * from "./cli/commands/funnel.js";
export * from "./cli/commands/abtest.js";
export * from "./cli/commands/pareto.js";
export * from "./cli/commands/technical.js";
export * from "./cli/commands/cluster.js";
export * from "./cli/commands/entropy.js";
export * from "./cli/commands/ngrams.js";
export * from "./cli/commands/pivot.js";
export * from "./cli/commands/unpivot.js";
export * from "./cli/commands/fuzzy-join.js";
export * from "./cli/commands/concat.js";
export * from "./cli/commands/generate.js";
export * from "./cli/commands/mask.js";
export * from "./cli/commands/test.js";
export * from "./cli/commands/serve.js";
export * from "./cli/commands/report.js";
export * from "./cli/completion.js";

// Planner & Optimizer
export * from "./planner/index.js";

// Analytics
export * from "./analytics/stats.js";
export * from "./analytics/reduce.js";
export * from "./analytics/schema-inference.js";
export * from "./analytics/semantic-types.js";
export * from "./analytics/validator.js";
export * from "./analytics/profiler.js";
export * from "./analytics/correlation.js";
export * from "./analytics/quantiles.js";
export * from "./analytics/outliers.js";
export * from "./analytics/crosstab.js";
export * from "./analytics/regression.js";
export * from "./analytics/rfm.js";
export * from "./analytics/cohort.js";
export * from "./analytics/funnel.js";
export * from "./analytics/abtest.js";
export * from "./analytics/pareto.js";
export * from "./analytics/technical.js";
export * from "./analytics/cluster.js";
export * from "./analytics/entropy.js";
export * from "./analytics/ngrams.js";

// DataOps & Security
export * from "./dataops/generate.js";
export * from "./dataops/mask.js";
export * from "./dataops/test-runner.js";
export * from "./dataops/server.js";
export * from "./dataops/report.js";
export * from "./dataops/fetch.js";

// Diff Engine & Storage
export * from "./diff/types.js";
export * from "./diff/key.js";
export * from "./diff/comparator.js";
export * from "./diff/hash.js";
export * from "./diff/schema.js";
export * from "./diff/engine.js";
export * from "./diff/reporter.js";
export * from "./diff/storage/memory-index.js";
export * from "./diff/storage/disk-index.js";
export * from "./diff/storage/spillable-index.js";

// UI & Viewer
export * from "./ui/colors.js";
export * from "./ui/chart.js";
export * from "./ui/viewer.js";
export * from "./cli/commands/view.js";

// Utilities
export * from "./utils/compression.js";
export * from "./utils/formatting.js";
export * from "./utils/progress.js";
export * from "./utils/heap.js";
export * from "./utils/ring-buffer.js";
export * from "./utils/keystore.js";

