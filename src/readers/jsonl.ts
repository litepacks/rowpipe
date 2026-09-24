import { openDecompressedReadStream } from "../utils/compression.js";
import { createInterface } from "node:readline";
import type { Readable } from "node:stream";
import { ParseError } from "../core/errors.js";
import { RowErrorHandler } from "../core/error-handler.js";
import type {
  DataBatch,
  DataStream,
  InspectionMetadata,
  ReaderOptions,
  Row,
  TabularReader,
} from "../core/types.js";

export interface JSONLReaderOptions extends ReaderOptions {
  filePath?: string;
}

export class JSONLReader implements TabularReader {
  private input: Readable | string;
  private options: JSONLReaderOptions;

  constructor(input: Readable | string, options: JSONLReaderOptions = {}) {
    this.input = input;
    this.options = { ...options };
    if (typeof input === "string") {
      this.options.filePath = input;
    }
  }

  private getInputStream(opts?: ReaderOptions): Readable {
    return openDecompressedReadStream(this.input, { ...this.options, ...opts });
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: JSONLReaderOptions = {
      ...this.options,
      ...options,
    };
    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);
    const stream = this.getInputStream(mergedOptions);

    const rl = createInterface({
      input: stream,
      crlfDelay: Infinity,
    });

    const errorHandler = new RowErrorHandler({
      strategy: mergedOptions.onError,
      badRowsLog: mergedOptions.badRowsLog,
      sourceFile: mergedOptions.filePath,
    });

    let lineNumber = 0;
    let rowsInCurrentBatch: Row[] = [];
    let globalOffset = 0;

    try {
      for await (const line of rl) {
        lineNumber++;
        const trimmed = line.trim();
        if (!trimmed) continue;

        let parsed: unknown;
        try {
          parsed = JSON.parse(trimmed);
        } catch (err) {
          errorHandler.handle(
            new ParseError(`Invalid JSON on line ${lineNumber}: ${(err as Error).message}`, {
              file: mergedOptions.filePath,
              line: lineNumber,
              row: lineNumber,
            }),
            { row: lineNumber, raw: trimmed, file: mergedOptions.filePath }
          );
          if (mergedOptions.onParseError) {
            mergedOptions.onParseError(err as Error, trimmed);
          }
          continue;
        }

        if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
          const err = new ParseError(
            `Expected JSON object on line ${lineNumber}, got ${Array.isArray(parsed) ? "array" : typeof parsed}`,
            {
              file: mergedOptions.filePath,
              line: lineNumber,
              row: lineNumber,
            }
          );
          errorHandler.handle(err, { row: lineNumber, raw: trimmed, file: mergedOptions.filePath });
          if (mergedOptions.onParseError) {
            mergedOptions.onParseError(err, trimmed);
          }
          continue;
        }

        rowsInCurrentBatch.push(parsed as Row);

        if (rowsInCurrentBatch.length >= batchSize) {
          yield {
            rows: rowsInCurrentBatch,
            offset: globalOffset,
          };
          globalOffset += rowsInCurrentBatch.length;
          rowsInCurrentBatch = [];
        }
      }

      if (rowsInCurrentBatch.length > 0) {
        yield {
          rows: rowsInCurrentBatch,
          offset: globalOffset,
        };
      }
    } finally {
      errorHandler.close();
    }
  }

  async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    let rowCount = 0;
    let columnNames: string[] = [];

    for await (const batch of this.read({ ...options, batchSize: 5000 })) {
      if (columnNames.length === 0 && batch.rows.length > 0 && batch.rows[0]) {
        columnNames = Object.keys(batch.rows[0]);
      }
      rowCount += batch.rows.length;
    }

    return {
      format: "JSONL",
      rowCount,
      columnCount: columnNames.length,
      columns: columnNames.map((name) => ({
        name,
        type: "string",
        nullPercentage: 0,
      })),
    };
  }
}
