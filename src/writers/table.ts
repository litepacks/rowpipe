import { createWriteStream, WriteStream } from "node:fs";
import type { DataBatch, DataStream, Row, TabularWriter } from "../core/types.js";
import { formatNumber } from "../utils/formatting.js";

export interface TableWriterOptions {
  style?: "unicode" | "ascii" | "compact";
  maxColWidth?: number;
  maxBufferRows?: number;
}

const BOX_STYLES = {
  unicode: {
    topLeft: "┌",
    topRight: "┐",
    bottomLeft: "└",
    bottomRight: "┘",
    horizontal: "─",
    vertical: "│",
    cross: "┼",
    topT: "┬",
    bottomT: "┴",
    leftT: "├",
    rightT: "┤",
  },
  ascii: {
    topLeft: "+",
    topRight: "+",
    bottomLeft: "+",
    bottomRight: "+",
    horizontal: "-",
    vertical: "|",
    cross: "+",
    topT: "+",
    bottomT: "+",
    leftT: "+",
    rightT: "+",
  },
  compact: {
    topLeft: "",
    topRight: "",
    bottomLeft: "",
    bottomRight: "",
    horizontal: "-",
    vertical: " ",
    cross: " ",
    topT: " ",
    bottomT: " ",
    leftT: "",
    rightT: "",
  },
};

/**
 * Formats an array of rows into a pretty Unicode / ASCII grid table string.
 */
export function formatTable(
  rows: Row[],
  options: TableWriterOptions = {}
): string {
  if (rows.length === 0) return "(empty dataset)\n";

  const style = BOX_STYLES[options.style || "unicode"] || BOX_STYLES.unicode;
  const maxColWidth = options.maxColWidth || 40;

  // Extract columns
  const colSet = new Set<string>();
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      colSet.add(k);
    }
  }
  const columns = Array.from(colSet);
  if (columns.length === 0) return "(no columns)\n";

  // Compute column widths
  const widths: Record<string, number> = {};
  for (const col of columns) {
    widths[col] = col.length;
  }

  for (const r of rows) {
    for (const col of columns) {
      const val = r[col];
      const str = val === null || val === undefined ? "" : typeof val === "object" ? JSON.stringify(val) : String(val);
      const displayLen = Math.min(maxColWidth, str.length);
      if (displayLen > (widths[col] || 0)) {
        widths[col] = displayLen;
      }
    }
  }

  function truncate(str: string, maxLen: number): string {
    if (str.length <= maxLen) return str;
    return str.slice(0, Math.max(0, maxLen - 1)) + "…";
  }

  const lines: string[] = [];

  // Top border
  if (style.topLeft) {
    const top = columns.map((col) => style.horizontal.repeat((widths[col] || 0) + 2)).join(style.topT);
    lines.push(`${style.topLeft}${top}${style.topRight}`);
  }

  // Header row
  const headerCells = columns.map((col) => {
    const padded = col.padEnd(widths[col] || 0);
    return ` ${padded} `;
  });
  lines.push(`${style.vertical}${headerCells.join(style.vertical)}${style.vertical}`);

  // Header separator
  const sep = columns.map((col) => style.horizontal.repeat((widths[col] || 0) + 2)).join(style.cross);
  lines.push(`${style.leftT}${sep}${style.rightT}`);

  // Data rows
  for (const r of rows) {
    const cells = columns.map((col) => {
      const val = r[col];
      const isNum = typeof val === "number";
      const rawStr = val === null || val === undefined ? "" : typeof val === "object" ? JSON.stringify(val) : String(val);
      const str = truncate(rawStr, widths[col] || 0);
      const padded = isNum ? str.padStart(widths[col] || 0) : str.padEnd(widths[col] || 0);
      return ` ${padded} `;
    });
    lines.push(`${style.vertical}${cells.join(style.vertical)}${style.vertical}`);
  }

  // Bottom border
  if (style.bottomLeft) {
    const bottom = columns.map((col) => style.horizontal.repeat((widths[col] || 0) + 2)).join(style.bottomT);
    lines.push(`${style.bottomLeft}${bottom}${style.bottomRight}`);
  }

  return lines.join("\n") + "\n";
}

/**
 * TabularWriter that outputs pretty-formatted terminal tables.
 */
export class TableWriter implements TabularWriter {
  private destination: string | NodeJS.WritableStream;
  private options: TableWriterOptions;
  private rows: Row[] = [];
  private outStream?: NodeJS.WritableStream;

  constructor(
    destination: string | NodeJS.WritableStream = "-",
    options: TableWriterOptions = {}
  ) {
    this.destination = destination;
    this.options = options;
  }

  async write(input: DataStream): Promise<void> {
    const maxBuffer = this.options.maxBufferRows || 5000;

    for await (const batch of input) {
      for (const row of batch.rows) {
        this.rows.push(row);
        if (this.rows.length >= maxBuffer) {
          break;
        }
      }
      if (this.rows.length >= maxBuffer) break;
    }

    const tableStr = formatTable(this.rows, this.options);

    if (this.destination === "-" || this.destination === process.stdout) {
      process.stdout.write(tableStr);
    } else if (typeof this.destination === "string") {
      const fsStream = createWriteStream(this.destination, { encoding: "utf8" });
      fsStream.write(tableStr);
      fsStream.end();
    } else {
      this.destination.write(tableStr);
    }
  }

  async close(): Promise<void> {
    this.rows = [];
  }
}
