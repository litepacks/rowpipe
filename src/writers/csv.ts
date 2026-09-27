import { closeWritableStream, openCompressedWriteStream } from "../utils/compression.js";
import type { Writable } from "node:stream";
import type { DataStream, TabularWriter, WriterOptions } from "../core/types.js";

export interface CSVWriterOptions extends WriterOptions {
  delimiter?: string;
  header?: boolean;
}

/**
 * Fast CSV field escaping using vectorized String.prototype.includes checks instead of RegExp.test.
 * Zero-allocation for standard numbers, booleans, and clean strings.
 */
export function formatCSVField(value: unknown, delimiter: string): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "number") {
    return "" + value;
  }
  if (typeof value === "string") {
    if (
      value.includes(delimiter) ||
      value.includes('"') ||
      value.includes("\n") ||
      value.includes("\r")
    ) {
      return `"${value.replace(/"/g, '""')}"`;
    }
    return value;
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  if (typeof value === "bigint") {
    return value.toString();
  }
  const str = typeof value === "object" ? JSON.stringify(value) : String(value);
  if (
    str.includes(delimiter) ||
    str.includes('"') ||
    str.includes("\n") ||
    str.includes("\r")
  ) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

export class CSVWriter implements TabularWriter {
  private output: Writable | string;
  private options: CSVWriterOptions;
  private createdStream?: Writable;

  constructor(output: Writable | string, options: CSVWriterOptions = {}) {
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

  private escapeField(value: unknown, delimiter: string, _quoteRegex?: RegExp): string {
    return formatCSVField(value, delimiter);
  }

  private async writeChunk(stream: Writable, data: string): Promise<void> {
    if (!stream.write(data)) {
      await new Promise<void>((resolve) => stream.once("drain", resolve));
    }
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const mergedOptions: CSVWriterOptions = {
      ...this.options,
      ...options,
    };
    const delimiter = mergedOptions.delimiter ?? ",";
    const writeHeader = mergedOptions.header !== false;

    const outStream = this.getOutputStream(mergedOptions);
    let headers: string[] | null = null;

    for await (const batch of dataStream) {
      if (batch.rows.length === 0) continue;

      if (headers === null) {
        // Collect all distinct keys from first batch to form header
        const keySet = new Set<string>();
        for (const row of batch.rows) {
          for (const key of Object.keys(row)) {
            keySet.add(key);
          }
        }
        headers = Array.from(keySet);

        if (writeHeader && headers.length > 0) {
          let headerLine = "";
          for (let i = 0; i < headers.length; i++) {
            if (i > 0) headerLine += delimiter;
            headerLine += formatCSVField(headers[i], delimiter);
          }
          headerLine += "\n";
          await this.writeChunk(outStream, headerLine);
        }
      }

      const rows = batch.rows;
      const bLen = rows.length;
      if (bLen === 0) continue;
      const hLen = headers.length;
      const lines = new Array<string>(bLen);

      for (let r = 0; r < bLen; r++) {
        const row = rows[r]!;
        let line = "";
        for (let i = 0; i < hLen; i++) {
          if (i > 0) line += delimiter;
          const val = row[headers[i]!];
          if (val === null || val === undefined) continue;
          if (typeof val === "number") {
            line += val;
          } else if (typeof val === "string") {
            if (
              val.includes(delimiter) ||
              val.includes('"') ||
              val.includes("\n") ||
              val.includes("\r")
            ) {
              line += `"${val.replace(/"/g, '""')}"`;
            } else {
              line += val;
            }
          } else if (typeof val === "boolean") {
            line += val ? "true" : "false";
          } else if (typeof val === "bigint") {
            line += val;
          } else {
            const s = typeof val === "object" ? JSON.stringify(val) : String(val);
            if (
              s.includes(delimiter) ||
              s.includes('"') ||
              s.includes("\n") ||
              s.includes("\r")
            ) {
              line += `"${s.replace(/"/g, '""')}"`;
            } else {
              line += s;
            }
          }
        }
        lines[r] = line;
      }

      await this.writeChunk(outStream, lines.join("\n") + "\n");
    }
  }

  async close(): Promise<void> {
    const s = this.createdStream || (typeof this.output !== "string" ? this.output : undefined);
    if (s && s !== process.stdout) {
      await closeWritableStream(s);
    }
  }
}
