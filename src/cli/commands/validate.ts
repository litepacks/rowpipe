import { readFile } from "node:fs/promises";
import {
  type SchemaValidatorAggregator,
  type ValidationSchemaDefinition,
  SchemaValidatorAggregator as Validator,
} from "../../analytics/validator.js";
import { InvalidArgumentError, ValidationError } from "../../core/errors.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { formatNumber, logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface ValidateCommandOptions {
  schema: string;
  from?: string;
  sheet?: string;
  path?: string;
  delimiter?: string;
  json?: boolean;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function validateCommand(
  inputPath = "-",
  options: ValidateCommandOptions
): Promise<void> {
  if (!options.schema) {
    throw new InvalidArgumentError("Missing required option: --schema <path_to_schema.json>");
  }

  const progress = new ProgressReporter(options);

  // Read and parse schema file
  let schemaContent: string;
  try {
    schemaContent = await readFile(options.schema, "utf8");
  } catch (err) {
    throw new InvalidArgumentError(
      `Failed to read schema file at "${options.schema}": ${(err as Error).message}`
    );
  }

  let schemaDef: ValidationSchemaDefinition;
  try {
    schemaDef = JSON.parse(schemaContent);
  } catch (err) {
    throw new InvalidArgumentError(
      `Invalid JSON in schema file "${options.schema}": ${(err as Error).message}`
    );
  }

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferFormatFromPath(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const inputStream = openReadableStream(inputPath);
  const reader = createReader(inputStream, {
    format: fromFormat,
    sheet: options.sheet,
    path: options.path,
    delimiter: options.delimiter,
    filePath: inputPath,
  });

  const validator = new Validator(schemaDef);
  const pipeline = createPipeline(reader);
  pipeline.onProgress((info) => progress.update(info));

  for await (const row of pipeline.rows()) {
    validator.add(row);
  }

  progress.done();
  const report = validator.result();

  if (options.json) {
    process.stdout.write(JSON.stringify(report, null, 2) + "\n");
  } else {
    process.stdout.write(`${formatNumber(report.totalRows)} rows scanned\n`);
    process.stdout.write(`${formatNumber(report.validRows)} valid\n`);
    process.stdout.write(`${formatNumber(report.invalidRows)} invalid\n`);

    if (report.violations.length > 0) {
      process.stdout.write("\nErrors\n\n");
      const groupedByCol = new Map<string, typeof report.violations>();
      for (const v of report.violations) {
        if (!groupedByCol.has(v.column)) groupedByCol.set(v.column, []);
        groupedByCol.get(v.column)!.push(v);
      }

      for (const [col, violations] of groupedByCol.entries()) {
        process.stdout.write(`${col}\n`);
        for (const v of violations) {
          process.stdout.write(`  ${v.rule}\n`);
          process.stdout.write(`  ${formatNumber(v.violationsCount)} violations\n\n`);
        }
      }
    }
  }

  logMemoryDebug();

  if (!report.isValid) {
    throw new ValidationError(
      `Schema validation failed: ${report.invalidRows} invalid rows found`,
      report.violations.map((v) => ({
        column: v.column,
        expected: v.rule,
        count: v.violationsCount,
        examples: v.sampleInvalidValues,
      })),
      report.totalRows,
      report.validRows,
      report.invalidRows
    );
  }
}
