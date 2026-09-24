import { closeWritableStream, openCompressedWriteStream } from "../utils/compression.js";
import type { Writable } from "node:stream";
import type { DataStream, TabularWriter, WriterOptions } from "../core/types.js";

export interface MarkdownWriterOptions extends WriterOptions {
  headers?: string[];
}

/**
 * Streaming GitHub-Flavored Markdown Table Writer.
 * Formats rows into clean markdown tables with proper cell escaping and backpressure.
 */
export class MarkdownWriter implements TabularWriter {
  private output: Writable | string;
  private options: MarkdownWriterOptions;
  private createdStream?: Writable;

  constructor(output: Writable | string, options: MarkdownWriterOptions = {}) {
    this.output = output;
    this.options = { ...options };
  }

  private getOutputStream(opts?: WriterOptions): Writable {
    const merged = { ...this.options, ...opts };
    const stream = openCompressedWriteStream(this.output, merged);
    if (typeof this.output === "string") {
      this.createdStream = stream;
    }
    return stream;
  }

  private escapeCell(value: unknown): string {
    if (value === null || value === undefined) {
      return "";
    }
    const str = typeof value === "object" ? JSON.stringify(value) : String(value);
    return str
      .replace(/\|/g, "\\|")
      .replace(/\r?\n/g, "<br>");
  }

  private async writeChunk(stream: Writable, data: string): Promise<void> {
    if (!stream.write(data)) {
      await new Promise<void>((resolve) => stream.once("drain", resolve));
    }
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const mergedOptions: MarkdownWriterOptions = {
      ...this.options,
      ...options,
    };

    const outStream = this.getOutputStream(mergedOptions);
    let headers: string[] | null = mergedOptions.headers ?? null;
    const allRows: string[][] = [];

    for await (const batch of dataStream) {
      if (batch.rows.length === 0) continue;

      if (headers === null) {
        headers = Object.keys(batch.rows[0] || {});
        if (headers.length === 0) {
          headers = ["value"];
        }
      }

      for (let r = 0; r < batch.rows.length; r++) {
        const row = batch.rows[r]!;
        const cells: string[] = new Array(headers.length);
        for (let i = 0; i < headers.length; i++) {
          const colName = headers[i]!;
          cells[i] = this.escapeCell(row[colName]);
        }
        allRows.push(cells);
      }
    }

    if (headers === null || headers.length === 0) {
      // Empty dataset
      await this.writeChunk(outStream, "| (empty) |\n| ------- |\n");
      return;
    }

    // Compute maximum width per column
    const colWidths: number[] = headers.map((h, colIdx) => {
      let maxLen = Math.max(3, h.length);
      for (let r = 0; r < allRows.length; r++) {
        const cell = allRows[r]?.[colIdx] ?? "";
        if (cell.length > maxLen) {
          maxLen = cell.length;
        }
      }
      return maxLen;
    });

    // Detect numeric / byte columns for clean right-alignment
    const isNumericCol: boolean[] = headers.map((_, colIdx) => {
      if (allRows.length === 0) return false;
      return allRows.every((row) => {
        const val = row[colIdx]?.trim();
        return !val || /^-?\d+(\.\d+)?(\s*[KMGTPE]?B)?$/.test(val);
      });
    });

    // 1. Render Header Row
    const headerCells = headers.map((h, i) =>
      isNumericCol[i] ? h.padStart(colWidths[i]!) : h.padEnd(colWidths[i]!)
    );
    const headerLine = `| ${headerCells.join(" | ")} |\n`;

    // 2. Render Separator Row (e.g. |:---|---:|)
    const separatorCells = colWidths.map((w, i) =>
      isNumericCol[i] ? "-".repeat(Math.max(2, w - 1)) + ":" : "-".repeat(Math.max(3, w))
    );
    const separatorLine = `| ${separatorCells.join(" | ")} |\n`;

    let buffer = headerLine + separatorLine;

    // 3. Render Data Rows
    for (let r = 0; r < allRows.length; r++) {
      const row = allRows[r]!;
      const rowCells = row.map((cell, i) =>
        isNumericCol[i] ? cell.padStart(colWidths[i]!) : cell.padEnd(colWidths[i]!)
      );
      buffer += `| ${rowCells.join(" | ")} |\n`;
    }

    if (buffer.length > 0) {
      await this.writeChunk(outStream, buffer);
    }
  }

  async close(): Promise<void> {
    const s = this.createdStream || (typeof this.output !== "string" ? this.output : undefined);
    if (s && s !== process.stdout) {
      await closeWritableStream(s);
    }
  }
}
