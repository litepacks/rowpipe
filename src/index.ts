// Core
export * from "./core/index.js";

// Readers
export {
  CSVReader,
  type CSVReaderOptions,
  JSONReader,
  type JSONReaderOptions,
  JSONLReader,
  type JSONLReaderOptions,
  NDJSONReader,
  type NDJSONReaderOptions,
  ParquetReader,
  type ParquetReaderOptions,
  ArrowReader,
  type ArrowReaderOptions,
  FeatherReader,
  type FeatherReaderOptions,
  mapArrowType,
  normalizeArrowValue,
  AvroReader,
  type AvroReaderOptions,
  mapAvroType,
  normalizeAvroValue,
  XMLReader,
  type XMLReaderOptions,
  coerceXmlPrimitive,
  XLSXReader,
  type XLSXReaderOptions,
  FileSystemReader,
  MultiFileReader,
  type MultiFileReaderOptions,
  createReader,
  inferFormatFromPath as inferReaderFormatFromPath,
  sniffFormatFromBuffer,
  sniffFormatFromFile,
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
  NDJSONWriter,
  MarkdownWriter,
  type MarkdownWriterOptions,
  ParquetWriter,
  type ParquetWriterOptions,
  ArrowWriter,
  type ArrowWriterOptions,
  FeatherWriter,
  type FeatherWriterOptions,
  AvroWriter,
  type AvroWriterOptions,
  inferAvroSchema,
  XMLWriter,
  type XMLWriterOptions,
  escapeXml,
  XLSXWriter,
  type XLSXWriterOptions,
  TableWriter,
  type TableWriterOptions,
  formatTable,
  createWriter,
  inferFormatFromPath as inferWriterFormatFromPath,
} from "./writers/index.js";

// Transforms
export * from "./transforms/index.js";
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
export * from "./cli/commands/formats.js";
export * from "./cli/completion.js";

// Planner & Optimizer
export * from "./planner/index.js";

// Analytics
export * from "./analytics/index.js";


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
export { findClosestMatch } from "./utils/fuzzy.js";
// MCP (Model Context Protocol) Server
export * from "./mcp/index.js";

// Default export for fluent programmatic API
import { rowpipe } from "./core/pipeline.js";
export default rowpipe;
