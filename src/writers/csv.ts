import { closeWritableStream, openCompressedWriteStream } from "../utils/compression.js";
import type { Writable } from "node:stream";
import type { DataStream, TabularWriter, WriterOptions } from "../core/types.js";

export interface CSVWriterOptions extends WriterOptions {
  delimiter?: string;
  header?: boolean;
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

  private escapeField(value: unknown, delimiter: string, quoteRegex: RegExp): string {
    if (value === null || value === undefined) {
      return "";
    }
    if (typeof value === "number" || typeof value === "bigint") {
      return value.toString();
    }
    if (typeof value === "boolean") {
      return value ? "true" : "false";
    }
    const str = typeof value === "string" ? value : (typeof value === "object" ? JSON.stringify(value) : String(value));
    if (quoteRegex.test(str)) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
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
    const escapedDelim = delimiter.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const quoteRegex = new RegExp(`["\r\n${escapedDelim}]`);

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
            headerLine += this.escapeField(headers[i], delimiter, quoteRegex);
          }
          headerLine += "\n";
          await this.writeChunk(outStream, headerLine);
        }
      }

      let chunkText = "";
      const hLen = headers.length;
      for (const row of batch.rows) {
        for (let i = 0; i < hLen; i++) {
          if (i > 0) chunkText += delimiter;
          chunkText += this.escapeField(row[headers[i]!], delimiter, quoteRegex);
        }
        chunkText += "\n";
      }

      if (chunkText.length > 0) {
        await this.writeChunk(outStream, chunkText);
      }
    }
  }

  async close(): Promise<void> {
    const s = this.createdStream || (typeof this.output !== "string" ? this.output : undefined);
    if (s && s !== process.stdout) {
      await closeWritableStream(s);
    }
  }
}
