export function formatNumber(num: number | null | undefined): string {
  if (num === null || num === undefined || Number.isNaN(num)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US").format(num);
}

export function formatDecimal(num: number | null | undefined, digits = 2): string {
  if (num === null || num === undefined || Number.isNaN(num)) {
    return "-";
  }
  return new Intl.NumberFormat("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: digits,
  }).format(num);
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes === null || bytes === undefined || Number.isNaN(bytes) || bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB", "TB", "PB"];
  const absBytes = Math.abs(bytes);
  const i = Math.min(sizes.length - 1, Math.floor(Math.log(absBytes) / Math.log(k)));
  const sign = bytes < 0 ? "-" : "";
  return `${sign}${Number.parseFloat((absBytes / k ** i).toFixed(2))} ${sizes[i]}`;
}

export function parseBytes(input: string | number | null | undefined): number {
  if (input === null || input === undefined) return 0;
  if (typeof input === "number") return input;
  const str = String(input).trim().toUpperCase();
  const match = str.match(/^([\d.]+)\s*([KMGTPE]?B?)$/);
  if (!match) {
    const num = Number.parseFloat(str);
    return Number.isNaN(num) ? 0 : num;
  }
  const val = Number.parseFloat(match[1]!);
  const unit = match[2] || "B";
  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1024,
    K: 1024,
    MB: 1024 ** 2,
    M: 1024 ** 2,
    GB: 1024 ** 3,
    G: 1024 ** 3,
    TB: 1024 ** 4,
    T: 1024 ** 4,
    PB: 1024 ** 5,
    P: 1024 ** 5,
  };
  return Math.round(val * (multipliers[unit] ?? 1));
}

export function formatTable(
  headers: string[],
  rows: string[][],
  alignments?: Array<"left" | "right">
): string {
  if (headers.length === 0) return "";

  const colWidths = headers.map((h) => h.length);

  for (const row of rows) {
    for (let i = 0; i < headers.length; i++) {
      const cell = row[i] || "";
      if (cell.length > (colWidths[i] || 0)) {
        colWidths[i] = cell.length;
      }
    }
  }

  const formatCell = (text: string, width: number, align: "left" | "right") => {
    if (align === "right") {
      return text.padStart(width);
    }
    return text.padEnd(width);
  };

  const headerLine = headers
    .map((h, i) =>
      formatCell(h, colWidths[i]!, alignments?.[i] || "left")
    )
    .join("   ");

  const rowLines = rows.map((row) =>
    headers
      .map((_, i) =>
        formatCell(row[i] || "", colWidths[i]!, alignments?.[i] || "left")
      )
      .join("   ")
  );

  return [headerLine, ...rowLines].join("\n");
}

export function logMemoryDebug(): void {
  if (process.env["ROWPIPE_DEBUG_MEMORY"] === "1") {
    const mem = process.memoryUsage();
    process.stderr.write(
      `\n[ROWPIPE DEBUG MEMORY]\n  Peak RSS:  ${formatBytes(mem.rss)}\n  Heap Used: ${formatBytes(mem.heapUsed)}\n  Heap Total: ${formatBytes(mem.heapTotal)}\n  External:  ${formatBytes(mem.external)}\n\n`
    );
  }
}

/**
 * Custom JSON serializer replacer supporting BigInt, Buffer/Uint8Array, and Date safely.
 */
export function safeJsonReplacer(_key: string, value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) {
    return Buffer.from(value).toString("base64");
  }
  return value;
}

export function safeJsonStringify(value: unknown, space?: number | string): string {
  return JSON.stringify(value, safeJsonReplacer, space);
}
