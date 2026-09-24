import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import { formatNumber } from "../../utils/formatting.js";
import { computeNgrams, type NgramOptions } from "../../analytics/ngrams.js";
import { formatTable } from "../../writers/table.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface NgramsCommandOptions extends UnifiedPipelineCliOptions, NgramOptions {
  n?: number;
  top?: number;
  stopwords?: boolean;
  minFreq?: number;
  caseSensitive?: boolean;
}

export async function ngramsCommand(
  inputPath = "-",
  columnName?: string,
  options: NgramsCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "csv";

  const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
  const reader = createReader(readerInput, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  const result = await computeNgrams(pipeline.batches(), {
    col: columnName || options.col,
    n: options.n ? Number(options.n) : 2,
    top: options.top ? Number(options.top) : 20,
    stopwords: options.stopwords,
    minFreq: options.minFreq ? Number(options.minFreq) : 1,
    caseSensitive: options.caseSensitive,
  });
  progress.done();

  if (options.json) {
    console.log(JSON.stringify(result, null, 2));
    return;
  }

  const rows = result.formattedRows;

  const isStructuredOutput = Boolean(options.to || options.output);
  if (isStructuredOutput) {
    let toFormat = options.to?.toLowerCase();
    if (!toFormat && options.output && options.output !== "-") {
      toFormat = inferWriterFormat(options.output) ?? undefined;
    }
    if (!toFormat) toFormat = "csv";

    const writer = createWriter(options.output || "-", {
      format: toFormat,
      delimiter: options.delimiter,
      ...options,
    });

    async function* singleBatch() {
      yield { rows, offset: 0 };
    }
    await writer.write(singleBatch());
    if (writer.close) await writer.close();
  } else {
    const gramLabel = result.n === 1 ? "Unigram" : result.n === 2 ? "Bigram" : result.n === 3 ? "Trigram" : `${result.n}-Gram`;
    process.stdout.write(`\n--- Top ${gramLabel}s in '${result.column}' (Total: ${formatNumber(result.total_ngrams)}, Unique: ${formatNumber(result.unique_ngrams)}) ---\n`);
    process.stdout.write(formatTable(rows, { style: "unicode" }));
    process.stdout.write("\n");
  }
}
