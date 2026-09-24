import { DatasetStatsAggregator } from "../../analytics/stats.js";
import { createPipeline } from "../../core/pipeline.js";
import { createReader, inferFormatFromPath } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import { formatDecimal, formatNumber, logMemoryDebug } from "../../utils/formatting.js";
import { ProgressReporter } from "../../utils/progress.js";

export interface StatsCommandOptions {
  column?: string;
  fast?: boolean;
  exact?: boolean;
  json?: boolean;
  from?: string;
  sheet?: string;
  path?: string;
  delimiter?: string;
  batchSize?: string | number;
  quiet?: boolean;
  noProgress?: boolean;
}

export async function statsCommand(
  inputPath = "-",
  options: StatsCommandOptions = {}
): Promise<void> {
  const progress = new ProgressReporter(options);

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
    batchSize: Number(options.batchSize) || 2000,
    filePath: inputPath,
  });

  const statsAgg = new DatasetStatsAggregator({ column: options.column });
  const pipeline = createPipeline(reader);
  pipeline.onProgress((info) => progress.update(info));

  for await (const row of pipeline.rows()) {
    statsAgg.add(row);
  }

  progress.done();
  const res = statsAgg.result();

  if (options.json) {
    process.stdout.write(JSON.stringify(res, null, 2) + "\n");
  } else {
    process.stdout.write(`Total Rows: ${formatNumber(res.totalRows)}\n\n`);

    for (const [colName, colStat] of Object.entries(res.columns)) {
      process.stdout.write(`${colName} (${colStat.type})\n`);
      process.stdout.write("----------------------------------------\n");

      if (colStat.type === "numeric" && colStat.numeric) {
        const n = colStat.numeric;
        process.stdout.write(`  count        ${formatNumber(n.count)}\n`);
        process.stdout.write(`  null         ${formatNumber(n.nullCount)}\n`);
        process.stdout.write(`  min          ${formatDecimal(n.min)}\n`);
        process.stdout.write(`  max          ${formatDecimal(n.max)}\n`);
        process.stdout.write(`  sum          ${formatDecimal(n.sum)}\n`);
        process.stdout.write(`  mean         ${formatDecimal(n.mean)}\n`);
        process.stdout.write(`  stddev       ${formatDecimal(n.stddev)}\n`);
        process.stdout.write(`  variance     ${formatDecimal(n.variance)}\n`);
        process.stdout.write(`  distinct*    ~${formatNumber(n.approxDistinct)}\n\n`);
      } else if (colStat.type === "string" && colStat.string) {
        const s = colStat.string;
        process.stdout.write(`  count        ${formatNumber(s.count)}\n`);
        process.stdout.write(`  null         ${formatNumber(s.nullCount)}\n`);
        process.stdout.write(`  empty        ${formatNumber(s.emptyCount)}\n`);
        process.stdout.write(`  min length   ${formatNumber(s.minLength)}\n`);
        process.stdout.write(`  max length   ${formatNumber(s.maxLength)}\n`);
        process.stdout.write(`  avg length   ${formatDecimal(s.avgLength, 1)}\n`);
        process.stdout.write(`  distinct*    ~${formatNumber(s.approxDistinct)}\n`);
        if (s.topValues.length > 0) {
          process.stdout.write(`  top values:\n`);
          for (const tv of s.topValues) {
            process.stdout.write(`    - "${tv.value}": ${formatNumber(tv.count)}\n`);
          }
        }
        process.stdout.write("\n");
      }
    }

    process.stdout.write("* approximate\n");
  }

  logMemoryDebug();
}
