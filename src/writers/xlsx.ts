import type { Writable } from "node:stream";
import writeXlsxFile from "write-excel-file/node";
import type { DataStream, TabularWriter, WriterOptions } from "../core/types.js";

export interface XLSXWriterOptions extends WriterOptions {
  sheet?: string;
}

export class XLSXWriter implements TabularWriter {
  private output: Writable | string;
  private options: XLSXWriterOptions;

  constructor(output: Writable | string, options: XLSXWriterOptions = {}) {
    this.output = output;
    this.options = { ...options };
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const mergedOptions: XLSXWriterOptions = {
      ...this.options,
      ...options,
    };
    const sheetName = mergedOptions.sheet || "Sheet1";

    let headers: string[] | null = null;
    const grid: Array<Array<{ value?: string | number | boolean | Date; fontWeight?: string }>> = [];

    for await (const batch of dataStream) {
      if (batch.rows.length === 0) continue;

      if (headers === null) {
        const keySet = new Set<string>();
        for (const row of batch.rows) {
          for (const key of Object.keys(row)) {
            keySet.add(key);
          }
        }
        headers = Array.from(keySet);

        // Header row
        grid.push(
          headers.map((h) => ({
            value: h,
            fontWeight: "bold",
          }))
        );
      }

      for (const row of batch.rows) {
        grid.push(
          headers.map((h) => {
            const val = row[h];
            if (val === null || val === undefined || val === "") {
              return { value: undefined };
            }
            if (typeof val === "number" || typeof val === "boolean" || typeof val === "string") {
              return { value: val };
            }
            if (val instanceof Date) {
              return { value: val };
            }
            return { value: String(val) };
          })
        );
      }
    }

    if (grid.length === 0) {
      grid.push([{ value: "" }]);
    }

    const writerInstance = (writeXlsxFile as unknown as (data: unknown, opts: unknown) => {
      toFile: (path: string) => Promise<void>;
      toStream: (stream: unknown) => Promise<void>;
    })(grid, { sheet: sheetName });

    if (typeof this.output === "string") {
      await writerInstance.toFile(this.output);
    } else {
      await writerInstance.toStream(this.output);
    }
  }

  async close(): Promise<void> {
    // Handled on write
  }
}
