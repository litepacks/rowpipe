import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { XLSXReader } from "../../readers/xlsx.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { openReadableStream, openWritableStream } from "../../utils/compression.js";
import { formatNumber, logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface ConvertCommandOptions {
  from?: string;
  to?: string;
  sheet?: string;
  allSheets?: boolean;
  outDir?: string;
  path?: string;
  delimiter?: string;
  header?: boolean;
  gzip?: boolean;
  brotli?: boolean;
  zstd?: boolean;
  deflate?: boolean;
  compress?: string;
  compression?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
  onError?: "abort" | "skip" | "log";
  badRowsLog?: string;
}

export async function convertCommand(
  inputPath = "-",
  outputPath?: string,
  options: ConvertCommandOptions = {}
): Promise<void> {
  const effectiveBatchSize = Number(options.batchSize) || 1000;

  // Determine input format
  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  // Handle multi-sheet export if --all-sheets is specified
  if (options.allSheets && fromFormat === "xlsx" && inputPath !== "-") {
    const outDir = options.outDir || outputPath || "./";
    const toFormat = (options.to?.toLowerCase() || "csv").replace(/^\./, "");

    await mkdir(outDir, { recursive: true });

    const xlsxReader = new XLSXReader(inputPath);
    const sheets = await xlsxReader.getParsedSheets();

    if (sheets.length === 0) {
      process.stderr.write("No sheets found in workbook.\n");
      return;
    }

    process.stderr.write(`Exporting ${sheets.length} sheets to "${outDir}"...\n`);

    for (const sheet of sheets) {
      const sheetName = sheet.sheet;
      const targetFileName = `${sheetName}.${toFormat}`;
      const targetFilePath = join(outDir, targetFileName);

      const sheetReader = createReader(inputPath, {
        format: "xlsx",
        sheet: sheetName,
        batchSize: effectiveBatchSize,
        filePath: inputPath,
      });

      const sheetWriter = createWriter(targetFilePath, {
        format: toFormat,
        delimiter: options.delimiter,
        header: options.header,
        ...options,
      });

      const pipeline = createPipeline(sheetReader, { batchSize: effectiveBatchSize });
      await pipeline.to(sheetWriter);

      const rowCount = Math.max(0, sheet.data.length - 1);
      process.stderr.write(
        `  ✓ Sheet "${sheetName}" -> ${targetFilePath} (${formatNumber(rowCount)} rows)\n`
      );
    }

    process.stderr.write("Done.\n");
    logMemoryDebug();
    return;
  }

  // Determine output format
  let toFormat = options.to?.toLowerCase();
  if (!toFormat && outputPath && outputPath !== "-") {
    toFormat = inferWriterFormat(outputPath) ?? undefined;
  }
  if (!toFormat) {
    toFormat = "csv";
  }

  const progress = new ProgressReporter(options);
  const effectiveOutput = outputPath || "-";

  const reader = createReader(inputPath, {
    format: fromFormat,
    sheet: options.sheet,
    path: options.path,
    delimiter: options.delimiter,
    header: options.header,
    filePath: inputPath,
    ...options,
    batchSize: effectiveBatchSize,
  });

  const writer = createWriter(effectiveOutput, {
    format: toFormat,
    delimiter: options.delimiter,
    header: options.header,
    sheet: options.sheet,
    ...options,
  });

  const pipeline = createPipeline(reader, { batchSize: effectiveBatchSize });
  pipeline.onProgress((info) => progress.update(info));

  await pipeline.to(writer);
  progress.done();
  logMemoryDebug();
}
