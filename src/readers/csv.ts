import { openDecompressedReadStream } from "../utils/compression.js";
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

export interface CSVReaderOptions extends ReaderOptions {
  delimiter?: string;
  header?: boolean;
  filePath?: string;
}

/**
 * Streaming CSV Reader supporting RFC 4180 quotes, multiline cells,
 * custom & auto-detected delimiters, BOM removal, and exact error reporting.
 */
export class CSVReader implements TabularReader {
  private input: Readable | string;
  private options: CSVReaderOptions;

  constructor(input: Readable | string, options: CSVReaderOptions = {}) {
    this.input = input;
    this.options = { ...options };
    if (typeof input === "string") {
      this.options.filePath = input;
    }
  }

  private getInputStream(opts?: ReaderOptions): Readable {
    return openDecompressedReadStream(this.input, { ...this.options, ...opts });
  }

  /**
   * Auto-detects delimiter by scanning candidate delimiters in a sample string.
   */
  public static detectDelimiter(sample: string): string {
    const candidates = [",", "\t", ";", "|"];
    const firstLine = sample.split(/\r?\n/)[0] || "";

    let bestDelimiter = ",";
    let maxCount = -1;

    for (const cand of candidates) {
      let count = 0;
      let inQuotes = false;
      for (let i = 0; i < firstLine.length; i++) {
        const ch = firstLine[i];
        if (ch === '"') {
          inQuotes = !inQuotes;
        } else if (ch === cand && !inQuotes) {
          count++;
        }
      }
      if (count > maxCount) {
        maxCount = count;
        bestDelimiter = cand;
      }
    }

    return maxCount > 0 ? bestDelimiter : ",";
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: CSVReaderOptions = {
      ...this.options,
      ...options,
    };
    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);
    const hasHeader = mergedOptions.header !== false;
    let delimiter = mergedOptions.delimiter;

    const errorHandler = new RowErrorHandler({
      strategy: mergedOptions.onError,
      badRowsLog: mergedOptions.badRowsLog,
      sourceFile: mergedOptions.filePath,
    });

    const stream = this.getInputStream(mergedOptions);

    let buffer = "";
    let headers: string[] | null = null;
    let isFirstChunk = true;
    let rowsInCurrentBatch: Row[] = [];
    let globalRowOffset = 0;
    let rowNumber = 0;
    let byteOffset = 0;
    let currentField = "";
    let currentRowFields: string[] = [];
    let inQuotes = false;
    let cursor = 0;

    try {
      for await (const chunk of stream) {
        const chunkStr: string =
          typeof chunk === "string" ? chunk : (chunk as Buffer).toString("utf8");

        if (isFirstChunk) {
          isFirstChunk = false;
          // Remove UTF-8 BOM if present
          if (chunkStr.charCodeAt(0) === 0xfeff) {
            buffer = chunkStr.slice(1);
          } else {
            buffer = chunkStr;
          }

          // Auto-detect delimiter if not specified
          if (!delimiter) {
            delimiter = CSVReader.detectDelimiter(buffer);
          }
        } else {
          buffer += chunkStr;
        }

        const activeDelimiter = delimiter ?? ",";
        const delimLen = activeDelimiter.length;
        const isSingleCharDelim = delimLen === 1;
        const delimCode = activeDelimiter.charCodeAt(0);

        let fieldStart = cursor;

        while (cursor < buffer.length) {
          if (inQuotes) {
            const charCode = buffer.charCodeAt(cursor);
            if (charCode === 34 /* " */) {
              if (cursor + 1 < buffer.length) {
                if (buffer.charCodeAt(cursor + 1) === 34) {
                  // Escaped double quote ("")
                  currentField += buffer.slice(fieldStart, cursor) + '"';
                  cursor += 2;
                  byteOffset += 2;
                  fieldStart = cursor;
                  continue;
                } else {
                  // Closing quote
                  currentField += buffer.slice(fieldStart, cursor);
                  inQuotes = false;
                  cursor++;
                  byteOffset++;
                  fieldStart = cursor;
                  continue;
                }
              } else {
                // Reached end of current buffer inside quotes; wait for next chunk
                break;
              }
            } else {
              cursor++;
              byteOffset++;
              continue;
            }
          } else {
            const charCode = buffer.charCodeAt(cursor);

            if (charCode === 34 /* " */) {
              if (cursor === fieldStart && currentField.length === 0) {
                inQuotes = true;
                cursor++;
                byteOffset++;
                fieldStart = cursor;
                continue;
              } else {
                const err = new ParseError(
                  `Unexpected quote inside unquoted field at byte offset ${byteOffset}`,
                  {
                    file: mergedOptions.filePath,
                    row: rowNumber + 1,
                    column: currentRowFields.length + 1,
                    byteOffset,
                  }
                );
                errorHandler.handle(err, {
                  row: rowNumber + 1,
                  column: currentRowFields.length + 1,
                  byteOffset,
                  file: mergedOptions.filePath,
                });
                if (mergedOptions.onParseError) {
                  mergedOptions.onParseError(err);
                }

                // Recover by skipping until newline
                while (cursor < buffer.length && buffer.charCodeAt(cursor) !== 10 && buffer.charCodeAt(cursor) !== 13) {
                  cursor++;
                  byteOffset++;
                }
                if (cursor < buffer.length && buffer.charCodeAt(cursor) === 13 && cursor + 1 < buffer.length && buffer.charCodeAt(cursor + 1) === 10) {
                  cursor += 2;
                  byteOffset += 2;
                } else if (cursor < buffer.length) {
                  cursor++;
                  byteOffset++;
                }
                currentField = "";
                currentRowFields = [];
                inQuotes = false;
                fieldStart = cursor;
                continue;
              }
            }

            // Check for delimiter
            const isDelim = isSingleCharDelim
              ? charCode === delimCode
              : (buffer.length - cursor >= delimLen && buffer.startsWith(activeDelimiter, cursor));

            if (isDelim) {
              const fieldPart = buffer.slice(fieldStart, cursor);
              const val = currentField.length > 0 ? currentField + fieldPart : fieldPart;
              currentRowFields.push(val);
              currentField = "";
              cursor += delimLen;
              byteOffset += delimLen;
              fieldStart = cursor;
              continue;
            }

            // If remaining buffer is shorter than multi-char delimiter, wait for next chunk
            if (!isSingleCharDelim && buffer.length - cursor < delimLen) {
              break;
            }

            // Check for CRLF / LF newline
            if (charCode === 13 /* \r */ || charCode === 10 /* \n */) {
              if (charCode === 13 && cursor + 1 >= buffer.length) {
                // Wait for next chunk to check for \n
                break;
              }

              const fieldPart = buffer.slice(fieldStart, cursor);
              const val = currentField.length > 0 ? currentField + fieldPart : fieldPart;
              currentRowFields.push(val);
              currentField = "";

              if (charCode === 13 && cursor + 1 < buffer.length && buffer.charCodeAt(cursor + 1) === 10) {
                cursor += 2;
                byteOffset += 2;
              } else {
                cursor++;
                byteOffset++;
              }
              fieldStart = cursor;

              // Skip entirely empty row if it's trailing whitespace
              if (
                currentRowFields.length === 1 &&
                currentRowFields[0]?.trim() === "" &&
                cursor >= buffer.length
              ) {
                currentRowFields = [];
                continue;
              }

              if (hasHeader && headers === null) {
                headers = currentRowFields.map((h, i) =>
                  h.trim().length > 0 ? h.trim() : `col_${i + 1}`
                );
                currentRowFields = [];
                continue;
              }

              if (headers === null) {
                headers = currentRowFields.map((_, i) => `col_${i + 1}`);
              }

              // Construct Row object
              const rowObj: Row = {};
              for (let i = 0; i < headers.length; i++) {
                const key = headers[i] || `col_${i + 1}`;
                const val = currentRowFields[i] ?? "";
                rowObj[key] = val;
              }

              rowNumber++;
              rowsInCurrentBatch.push(rowObj);
              currentRowFields = [];

              if (rowsInCurrentBatch.length >= batchSize) {
                yield {
                  rows: rowsInCurrentBatch,
                  offset: globalRowOffset,
                };
                globalRowOffset += rowsInCurrentBatch.length;
                rowsInCurrentBatch = [];
              }
              continue;
            }

            cursor++;
            byteOffset++;
          }
        }

        // Compact processed buffer to keep memory bounded
        if (fieldStart < cursor) {
          currentField += buffer.slice(fieldStart, cursor);
        }
        buffer = buffer.slice(cursor);
        cursor = 0;
      }

      // Process remainder if any
      if (inQuotes) {
        const err = new ParseError("Unclosed quote at end of CSV stream", {
          file: mergedOptions.filePath,
          row: rowNumber + 1,
          byteOffset,
        });
        errorHandler.handle(err, { row: rowNumber + 1, byteOffset, file: mergedOptions.filePath });
        if (mergedOptions.onParseError) {
          mergedOptions.onParseError(err);
        }
      }

      if (currentField.length > 0 || currentRowFields.length > 0) {
        currentRowFields.push(currentField);

        if (hasHeader && headers === null) {
          headers = currentRowFields.map((h, i) =>
            h.trim().length > 0 ? h.trim() : `col_${i + 1}`
          );
        } else {
          if (headers === null) {
            headers = currentRowFields.map((_, i) => `col_${i + 1}`);
          }

          const rowObj: Row = {};
          for (let i = 0; i < headers.length; i++) {
            const key = headers[i] || `col_${i + 1}`;
            const val = currentRowFields[i] ?? "";
            rowObj[key] = val;
          }

          rowNumber++;
          rowsInCurrentBatch.push(rowObj);
        }
      }

      if (rowsInCurrentBatch.length > 0) {
        yield {
          rows: rowsInCurrentBatch,
          offset: globalRowOffset,
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
      format: "CSV",
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
