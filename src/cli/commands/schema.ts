import { SchemaInferenceAggregator } from "../../analytics/schema-inference.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { formatTable, logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface SchemaCommandOptions {
  from?: string;
  sheet?: string;
  path?: string;
  delimiter?: string;
  sample?: string | number;
  full?: boolean;
  json?: boolean;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function schemaCommand(
  inputPath = "-",
  options: SchemaCommandOptions = {}
): Promise<void> {
  const progress = new ProgressReporter(options);

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferFormatFromPath(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const sampleSize = options.full
    ? Number.POSITIVE_INFINITY
    : options.sample !== undefined
      ? Number(options.sample)
      : 10000;

  const inputStream = openReadableStream(inputPath);
  const reader = createReader(inputStream, {
    format: fromFormat,
    sheet: options.sheet,
    path: options.path,
    delimiter: options.delimiter,
    filePath: inputPath,
  });

  const schemaAgg = new SchemaInferenceAggregator({ sample: sampleSize });
  const pipeline = createPipeline(reader);
  pipeline.onProgress((info) => progress.update(info));

  for await (const row of pipeline.rows()) {
    schemaAgg.add(row);
  }

  progress.done();
  const res = schemaAgg.result();

  if (options.json) {
    process.stdout.write(JSON.stringify(res, null, 2) + "\n");
  } else {
    const tableRows = res.columns.map((col) => {
      let typeDisplay = col.type as string;
      if (col.semantic) {
        typeDisplay += ` (${col.semantic})`;
      }
      return [
        col.name,
        typeDisplay,
        col.nullable ? "true" : "false",
        `${col.confidence}%`,
      ];
    });

    process.stdout.write(
      formatTable(
        ["COLUMN", "TYPE", "NULLABLE", "CONFIDENCE"],
        tableRows,
        ["left", "left", "left", "right"]
      ) + "\n"
    );
  }

  logMemoryDebug();
}
