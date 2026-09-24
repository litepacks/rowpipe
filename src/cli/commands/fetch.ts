import { HttpReader, type HttpFetchOptions } from "../../dataops/fetch.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { ProgressReporter } from "../../utils/progress.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface FetchCommandOptions extends Omit<UnifiedPipelineCliOptions, "batchSize">, Omit<HttpFetchOptions, "batchSize"> {
  batchSize?: number | string;
  headersList?: string[];
}

export async function fetchCommand(
  url: string,
  options: FetchCommandOptions = {}
): Promise<void> {
  const parsedHeaders: Record<string, string> = {};
  if (options.headersList) {
    for (const h of options.headersList) {
      const idx = h.indexOf(":");
      if (idx !== -1) {
        parsedHeaders[h.slice(0, idx).trim()] = h.slice(idx + 1).trim();
      }
    }
  }

  const effectiveBatchSize = Number(options.batchSize) || 1000;
  const progress = new ProgressReporter(options);

  const reader = new HttpReader(url, {
    ...options,
    batchSize: effectiveBatchSize,
    headers: { ...parsedHeaders, ...options.headers },
  });

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

  const stream = reader.read();
  await writer.write(stream);
  if (writer.close) await writer.close();
  progress.done();
}
