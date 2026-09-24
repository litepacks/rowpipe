import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { countStream } from "../../transforms/count.js";
import { openReadableStream } from "../../utils/compression.js";
import { formatNumber, formatTable } from "../../utils/formatting.js";

export interface CountCommandOptions {
  by?: string | string[];
  distinct?: string;
  approx?: boolean;
  from?: string;
  sheet?: string;
  delimiter?: string;
  json?: boolean;
  quiet?: boolean;
}

export async function countCommand(
  inputPath = "-",
  options: CountCommandOptions = {}
): Promise<void> {
  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) {
    fromFormat = "csv";
  }

  const inputStream = openReadableStream(inputPath);
  const reader = createReader(inputStream, {
    format: fromFormat,
    sheet: options.sheet,
    delimiter: options.delimiter,
    batchSize: 5000,
    filePath: inputPath,
  });

  const result = await countStream(reader.read(), {
    by: options.by,
    distinct: options.distinct,
    approx: options.approx,
    json: options.json,
  });

  if (options.json) {
    process.stdout.write(JSON.stringify(result, null, 2) + "\n");
    return;
  }

  if (options.by && result.groups) {
    const headers = ["group", "count"];
    const rows = result.groups.map((g) => {
      const keyStr = Object.entries(g.key)
        .map(([k, v]) => `${k}=${v}`)
        .join(", ");
      return [keyStr, formatNumber(g.count)];
    });
    process.stdout.write(formatTable(headers, rows, ["left", "right"]) + "\n");
    return;
  }

  if (options.distinct) {
    const approxPrefix = result.isApproximate ? "~" : "";
    process.stdout.write(
      `distinct ${options.distinct}: ${approxPrefix}${formatNumber(result.distinctCount ?? 0)}\n`
    );
    return;
  }

  process.stdout.write(`${formatNumber(result.totalRows)}\n`);
}
