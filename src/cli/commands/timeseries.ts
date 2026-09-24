import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { ProgressReporter } from "../../utils/progress.js";
import {
  resampleTimeseries,
  parseTimeseriesAggSpecs,
  type GapFillMethod,
  type TimeseriesAggSpec,
} from "../../transforms/timeseries.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface TimeseriesCommandOptions extends UnifiedPipelineCliOptions {
  timeCol?: string;
  timeColumn?: string;
  interval?: string;
  fillGaps?: GapFillMethod;
  timeFormat?: "iso" | "epoch_ms" | "epoch_s" | "date_only";
}

export async function timeseriesCommand(
  inputPath = "-",
  options: TimeseriesCommandOptions = {}
): Promise<void> {
  const timeCol = options.timeCol || options.timeColumn;
  if (!timeCol) {
    throw new Error(
      "Time column is required for timeseries resampling (e.g. rowpipe timeseries data.csv --time-col timestamp --interval 1h)"
    );
  }

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

  // Build aggregation specs
  const aggs: TimeseriesAggSpec[] = [];
  if (options.agg) {
    const rawAggs = Array.isArray(options.agg) ? options.agg : [options.agg];
    aggs.push(...parseTimeseriesAggSpecs(rawAggs));
  }

  function addColAggs(fn: TimeseriesAggSpec["fn"], cols?: string | string[]) {
    if (!cols) return;
    const rawList = Array.isArray(cols) ? cols : [cols];
    const list = rawList.flatMap((item) => item.split(",").map((s) => s.trim()));
    for (const c of list) {
      if (c) aggs.push({ fn, column: c, alias: `${fn}_${c}` });
    }
  }

  addColAggs("avg", options.avg);
  addColAggs("sum", options.sum);
  addColAggs("min", options.min);
  addColAggs("max", options.max);
  addColAggs("first", options.first);
  addColAggs("last", options.last);

  if (options.count || aggs.length === 0) {
    aggs.push({ fn: "count", column: "*", alias: "count" });
  }

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  const resampledStream = pipeline.pipe(
    resampleTimeseries({
      timeCol,
      interval: options.interval || "1m",
      aggregations: aggs,
      fillGaps: options.fillGaps,
      timeFormat: options.timeFormat || "iso",
    })
  );

  let toFormat = options.to?.toLowerCase();
  if (options.json) {
    toFormat = "json";
  } else if (!toFormat && options.output && options.output !== "-") {
    toFormat = inferWriterFormat(options.output) ?? undefined;
  }
  if (!toFormat) toFormat = "csv";

  const writer = createWriter(options.output || "-", {
    format: toFormat,
    delimiter: options.delimiter,
    ...options,
  });

  await writer.write(resampledStream.batches());
  if (writer.close) await writer.close();
  progress.done();
}
