import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { formatTable } from "../../writers/table.js";
import { runDataTests, type DataTestOptions } from "../../dataops/test-runner.js";
import type { Row } from "../../core/types.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface TestCommandOptions extends Omit<UnifiedPipelineCliOptions, "unique">, DataTestOptions {
  failOnError?: boolean;
}

export async function testCommand(
  inputPath = "-",
  options: TestCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;

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

  const summary = await runDataTests(pipeline.batches(), {
    assert: options.assert,
    notNull: options.notNull,
    unique: typeof options.unique === "string" || Array.isArray(options.unique) ? options.unique : undefined,
    minRows: options.minRows !== undefined ? Number(options.minRows) : undefined,
    maxRows: options.maxRows !== undefined ? Number(options.maxRows) : undefined,
    maxErrors: options.maxErrors !== undefined ? Number(options.maxErrors) : 5,
    failFast: options.failFast,
  });

  if (options.json) {
    process.stdout.write(JSON.stringify(summary, null, 2) + "\n");
  } else {
    const tableRows: Row[] = summary.results.map((r) => ({
      rule: r.rule.description,
      status: r.passed ? "PASSED" : "FAILED",
      violations: r.violationsCount,
      sample_violation: r.sampleViolations.length > 0 ? JSON.stringify(r.sampleViolations[0]) : "-",
    }));

    process.stdout.write(formatTable(tableRows, { style: "unicode" }));
    process.stdout.write(
      `\nTest Result: ${summary.allPassed ? "SUCCESS" : "FAILED"} (${summary.passedRulesCount}/${summary.rulesCount} passed across ${summary.totalRows} rows)\n`
    );
  }

  if (reader.close) await reader.close();

  const failOnError = options.failOnError !== false;
  if (!summary.allPassed && failOnError) {
    process.exit(1);
  }
}
