import { promises as fs } from "node:fs";
import { generateHtmlReport, type ReportOptions } from "../../dataops/report.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ReportCommandOptions extends UnifiedPipelineCliOptions, ReportOptions {
  title?: string;
  output?: string;
}

export async function reportCommand(
  inputPath = "-",
  options: ReportCommandOptions = {}
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

  const html = await generateHtmlReport(reader, inputPath, {
    title: options.title,
    outputPath: options.output,
  });

  if (options.output && options.output !== "-") {
    await fs.writeFile(options.output, html, "utf-8");
    process.stdout.write(`\n✨ HTML Report generated: ${options.output}\n`);
  } else {
    process.stdout.write(html);
  }

  if (reader.close) await reader.close();
}
