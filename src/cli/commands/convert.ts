import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { XLSXReader } from "../../readers/xlsx.js";
import { createWriter, inferFormatFromPath as inferWriterFormat } from "../../writers/index.js";
import { formatTable } from "../../writers/table.js";
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
  dryRun?: boolean | string | number;
  preview?: boolean | string | number;
  json?: boolean;
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

  const isDryRun = Boolean(
    (options.dryRun !== undefined && options.dryRun !== false) ||
    (options.preview !== undefined && options.preview !== false)
  );

  // Handle multi-sheet export if --all-sheets is specified
  if (options.allSheets && fromFormat === "xlsx" && inputPath !== "-") {
    const outDir = options.outDir || outputPath || "./";
    const toFormat = (options.to?.toLowerCase() || "csv").replace(/^\./, "");

    const xlsxReader = new XLSXReader(inputPath);
    const sheets = await xlsxReader.getParsedSheets();

    if (sheets.length === 0) {
      process.stderr.write("No sheets found in workbook.\n");
      return;
    }

    if (isDryRun) {
      if (options.json) {
        process.stdout.write(
          JSON.stringify(
            {
              dryRun: true,
              mode: "all-sheets",
              source: inputPath,
              outDir,
              toFormat,
              sheetsCount: sheets.length,
              sheets: sheets.map((s) => ({
                name: s.sheet,
                rowCount: Math.max(0, s.data.length - 1),
                targetFile: join(outDir, `${s.sheet}.${toFormat}`),
              })),
            },
            null,
            2
          ) + "\n"
        );
        return;
      }

      process.stdout.write(`\n🔍 Pipeline Dry Run (Multi-Sheet):\n`);
      process.stdout.write(`  Source: ${inputPath} (xlsx)\n`);
      process.stdout.write(`  Target Directory: ${outDir}\n`);
      process.stdout.write(`  Target Format: ${toFormat}\n`);
      process.stdout.write(`  Sheets to export (${sheets.length}):\n`);
      for (const s of sheets) {
        process.stdout.write(
          `    • ${s.sheet} -> ${join(outDir, `${s.sheet}.${toFormat}`)} (${formatNumber(Math.max(0, s.data.length - 1))} rows)\n`
        );
      }
      process.stdout.write(`\n(No files were created - dry-run mode active)\n\n`);
      return;
    }

    await mkdir(outDir, { recursive: true });

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

  const effectiveOutput = outputPath || "-";

  if (isDryRun) {
    const rawCount = options.preview ?? options.dryRun;
    const previewCount =
      typeof rawCount === "number" && rawCount > 0
        ? rawCount
        : typeof rawCount === "string" && !Number.isNaN(Number(rawCount)) && Number(rawCount) > 0
          ? Number(rawCount)
          : 5;

    const reader = createReader(inputPath, {
      format: fromFormat,
      sheet: options.sheet,
      path: options.path,
      delimiter: options.delimiter,
      header: options.header,
      filePath: inputPath,
      ...options,
      batchSize: Math.max(previewCount, 10),
    });

    const pipeline = createPipeline(reader, { batchSize: Math.max(previewCount, 10) });
    const previewResult = await pipeline.dryRun(previewCount);

    if (options.json) {
      process.stdout.write(
        JSON.stringify(
          {
            dryRun: true,
            source: {
              path: inputPath,
              format: fromFormat,
            },
            target: {
              path: effectiveOutput,
              format: toFormat,
            },
            previewCount: previewResult.totalSampled,
            columns: previewResult.columns,
            types: previewResult.types,
            rows: previewResult.rows,
          },
          null,
          2
        ) + "\n"
      );
      return;
    }

    process.stdout.write(`\n🔍 Pipeline Dry Run / Preview:\n`);
    process.stdout.write(`  Source: ${inputPath} (format: ${fromFormat})\n`);
    process.stdout.write(`  Target: ${effectiveOutput} (format: ${toFormat})\n`);
    process.stdout.write(`  Sampled Rows: ${previewResult.totalSampled}\n\n`);

    if (previewResult.rows.length > 0) {
      process.stdout.write(formatTable(previewResult.rows));
      process.stdout.write(`\nInferred Column Types:\n`);
      for (const [col, colType] of Object.entries(previewResult.types)) {
        process.stdout.write(`  • ${col}: ${colType}\n`);
      }
    } else {
      process.stdout.write(`  (empty dataset - 0 rows read)\n`);
    }

    process.stdout.write(`\n(No files were written - dry-run mode active)\n\n`);
    return;
  }

  const progress = new ProgressReporter(options);

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
