import { SyntheticDataGenerator } from "../../dataops/generate.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { ProgressReporter } from "../../utils/progress.js";
import { InvalidArgumentError } from "../../core/errors.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface GenerateCommandOptions extends Omit<UnifiedPipelineCliOptions, "count"> {
  schema?: string;
  rows?: number | string;
  count?: number | string;
}

export async function generateCommand(
  schemaArg?: string,
  options: GenerateCommandOptions = {}
): Promise<void> {
  const schemaSpec = schemaArg || options.schema || "id:seq,name:name,email:email,age:int(18,70),created_at:date";
  const rowCount = Number(options.rows || options.count || 1000);
  const effectiveBatchSize = Number(options.batchSize) || 1000;

  if (Number.isNaN(rowCount) || rowCount <= 0) {
    throw new InvalidArgumentError(`Invalid row count: ${options.rows}`);
  }

  const progress = new ProgressReporter(options);
  const generator = new SyntheticDataGenerator(schemaSpec, rowCount, effectiveBatchSize);

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

  const stream = generator.read();
  await writer.write(stream);
  if (writer.close) await writer.close();
  progress.done();
}
