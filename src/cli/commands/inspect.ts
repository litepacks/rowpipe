import { stat } from "node:fs/promises";
import { DatasetStatsAggregator } from "../../analytics/stats.js";
import { SchemaInferenceAggregator } from "../../analytics/schema-inference.js";
import { InvalidArgumentError } from "../../core/errors.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import {
  formatBytes,
  formatDecimal,
  formatNumber,
  formatTable,
  logMemoryDebug,
} from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface InspectCommandOptions {
  sheet?: string;
  path?: string;
  delimiter?: string;
  json?: boolean;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function inspectCommand(
  inputPath = "-",
  options: InspectCommandOptions = {}
): Promise<void> {
  const progress = new ProgressReporter(options);

  let fileSize: number | undefined;
  if (inputPath !== "-") {
    try {
      const fileStat = await stat(inputPath);
      fileSize = fileStat.size;
    } catch {
      // Ignore if cannot stat
    }
  }

  const format =
    inputPath !== "-" ? inferFormatFromPath(inputPath) || "CSV" : "CSV";

  const stream = openReadableStream(inputPath);
  const reader = createReader(stream, {
    format,
    sheet: options.sheet,
    path: options.path,
    delimiter: options.delimiter,
    filePath: inputPath,
  });

  // If XLSX without a specific --sheet requested, show rich workbook & sheet breakdown
  if (format === "xlsx" && !options.sheet && reader.inspect) {
    const meta = await reader.inspect();
    if (options.json) {
      process.stdout.write(
        JSON.stringify(
          {
            file: inputPath === "-" ? "stdin" : inputPath,
            format: "XLSX",
            sizeBytes: fileSize,
            sizeFormatted: fileSize !== undefined ? formatBytes(fileSize) : undefined,
            sheetsCount: meta.sheets?.length ?? 0,
            sheets: meta.sheets,
          },
          null,
          2
        ) + "\n"
      );
      logMemoryDebug();
      return;
    }

    process.stdout.write(`File: ${inputPath === "-" ? "stdin" : inputPath}\n`);
    process.stdout.write(`Format: XLSX\n`);
    if (fileSize !== undefined) {
      process.stdout.write(`Size: ${formatBytes(fileSize)}\n`);
    }

    if (meta.sheets && meta.sheets.length > 0) {
      process.stdout.write(`Sheets: ${meta.sheets.length}\n\n`);
      const sheetRows = meta.sheets.map((s) => {
        const previewCols = (s.columns || []).slice(0, 5).join(", ") + (s.columns && s.columns.length > 5 ? ", ..." : "");
        return [
          s.name,
          formatNumber(s.rowCount),
          s.columnCount !== undefined ? String(s.columnCount) : "-",
          previewCols,
        ];
      });

      process.stdout.write(
        formatTable(
          ["SHEET", "ROWS", "COLUMNS", "PREVIEW COLUMNS"],
          sheetRows,
          ["left", "right", "right", "left"]
        ) + "\n\nTip: Run with --sheet <name> to inspect a specific sheet in full detail.\n"
      );
    }
    logMemoryDebug();
    return;
  }

  // General inspection across stream
  const schemaAgg = new SchemaInferenceAggregator();
  const statsAgg = new DatasetStatsAggregator();

  const pipeline = createPipeline(reader);
  pipeline.onProgress((info) => progress.update(info));

  for await (const row of pipeline.rows()) {
    schemaAgg.add(row);
    statsAgg.add(row);
  }

  progress.done();

  const schemaRes = schemaAgg.result();
  const statsRes = statsAgg.result();

  const totalRows = schemaRes.totalRowsScanned;
  const columnCount = schemaRes.columns.length;

  const columnDetails = schemaRes.columns.map((col) => {
    const nullPct =
      col.sampleCount > 0 ? (col.nullCount / col.sampleCount) * 100 : 0;
    const colStats = statsRes.columns[col.name];
    const distinctCount =
      colStats?.numeric?.approxDistinct ?? colStats?.string?.approxDistinct ?? 0;
    const uniquePct =
      totalRows > 0 ? Math.min(100, (distinctCount / totalRows) * 100) : 0;

    return {
      name: col.name,
      type: col.type,
      nullPct,
      uniquePct,
      semantic: col.semantic,
    };
  });

  if (options.json) {
    const resultJson = {
      file: inputPath === "-" ? "stdin" : inputPath,
      format: format.toUpperCase(),
      sheet: options.sheet,
      sizeBytes: fileSize,
      sizeFormatted: fileSize !== undefined ? formatBytes(fileSize) : undefined,
      rows: totalRows,
      columnsCount: columnCount,
      columns: columnDetails.map((c) => ({
        name: c.name,
        type: c.type,
        nullPercentage: Math.round(c.nullPct * 10) / 10,
        approxUniquePercentage: Math.round(c.uniquePct * 10) / 10,
        semantic: c.semantic,
      })),
    };
    process.stdout.write(JSON.stringify(resultJson, null, 2) + "\n");
  } else {
    process.stdout.write(`File: ${inputPath === "-" ? "stdin" : inputPath}\n`);
    process.stdout.write(`Format: ${format.toUpperCase()}`);
    if (options.sheet) {
      process.stdout.write(` (Sheet: ${options.sheet})`);
    }
    process.stdout.write("\n");

    if (fileSize !== undefined) {
      process.stdout.write(`Size: ${formatBytes(fileSize)}\n`);
    }
    process.stdout.write(`Rows: ${formatNumber(totalRows)}\n`);
    process.stdout.write(`Columns: ${columnCount}\n\n`);

    const tableRows = columnDetails.map((c) => [
      c.name,
      c.type,
      `${formatDecimal(c.nullPct, 1)}%`,
      `${formatDecimal(c.uniquePct, 1)}%`,
    ]);

    process.stdout.write(
      formatTable(
        ["COLUMN", "TYPE", "NULL", "UNIQUE*"],
        tableRows,
        ["left", "left", "right", "right"]
      ) + "\n\n* approximate\n"
    );
  }

  logMemoryDebug();
}
