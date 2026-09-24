import { createReader, inferFormatFromPath as inferReaderFormat, isGlobPattern } from "../../readers/index.js";
import { TerminalViewer, TerminalViewerOptions } from "../../ui/viewer.js";
import { openReadableStream } from "../../utils/compression.js";
import { executePlannedPipeline, optimizePipeline } from "../../planner/index.js";
import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ViewCommandOptions extends UnifiedPipelineCliOptions {
  title?: string;
  maxBuffer?: string | number;
  initialRows?: string | number;
  interactive?: boolean;
}

/**
 * Handles `rowpipe view [input] [options]`
 */
export async function viewCommand(
  inputPath = "-",
  options: ViewCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const maxBufferRows = options.maxBuffer ? Number(options.maxBuffer) : 25000;
  const initialRows = options.initialRows ? Number(options.initialRows) : 200;

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    table: options.table,
    addFilename: options.addFilename,
    fileCol: options.fileCol,
    ...options,
    batchSize: effectiveBatchSize,
  });

  // Apply any upstream transformations (e.g. initial filter or select)
  const operations = buildOperationsFromOptions(options);
  const plan = optimizePipeline(operations);
  const pipeline = executePlannedPipeline(reader, plan, { batchSize: effectiveBatchSize });

  const viewer = new TerminalViewer(pipeline.batches(), {
    title: options.title,
    filePath: inputPath !== "-" ? inputPath : "Standard Input (stdin)",
    maxBufferRows,
    initialRows,
    interactive: options.interactive !== false,
  });

  try {
    await viewer.run();
  } finally {
    if (reader.close) {
      await reader.close();
    }
  }
}
