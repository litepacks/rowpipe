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

export interface JSONReaderOptions extends ReaderOptions {
  path?: string;
  filePath?: string;
}

/**
 * Incremental streaming JSON parser for top-level arrays [ {...}, ... ]
 * and nested array paths (e.g. data.results).
 * Never loads the full JSON file into memory.
 */
export class JSONReader implements TabularReader {
  private input: Readable | string;
  private options: JSONReaderOptions;

  constructor(input: Readable | string, options: JSONReaderOptions = {}) {
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
    const mergedOptions: JSONReaderOptions = {
      ...this.options,
      ...options,
    };
    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);
    const targetPath = mergedOptions.path ?? "";
    const stream = this.getInputStream(mergedOptions);

    const errorHandler = new RowErrorHandler({
      strategy: mergedOptions.onError,
      badRowsLog: mergedOptions.badRowsLog,
      sourceFile: mergedOptions.filePath,
    });

    let buffer = "";
    let cursor = 0;
    let byteOffset = 0;
    let foundTargetArray = !targetPath; // if no path, we look for first '['
    let currentPath: string[] = [];

    // State machine for array item scanning
    let inArray = false;
    let inString = false;
    let isEscaped = false;
    let objectDepth = 0;
    let objectStartIndex = -1;

    let rowsInCurrentBatch: Row[] = [];
    let globalOffset = 0;
    let rowIndex = 0;

    // Helper for path navigation if targetPath is set
    const targetPathParts = targetPath ? targetPath.split(".") : [];

    try {
      for await (const chunk of stream) {
        const chunkStr =
          typeof chunk === "string" ? chunk : (chunk as Buffer).toString("utf8");
        buffer += chunkStr;

        while (cursor < buffer.length) {
          const char = buffer[cursor];

          if (isEscaped) {
            isEscaped = false;
            cursor++;
            byteOffset++;
            continue;
          }

          if (char === "\\") {
            isEscaped = true;
            cursor++;
            byteOffset++;
            continue;
          }

          if (char === '"') {
            inString = !inString;
            cursor++;
            byteOffset++;
            continue;
          }

          if (inString) {
            cursor++;
            byteOffset++;
            continue;
          }

          // We are outside of any string literal
          if (!foundTargetArray) {
            // If we need to find target path (e.g. data.results)
            // Look for keys and match path parts
            if (char === "{") {
              cursor++;
              byteOffset++;
              continue;
            } else if (char === "[") {
              // Check if current path matches targetPath
              // If path matched or we hit array at root
              foundTargetArray = true;
              inArray = true;
              cursor++;
              byteOffset++;
              continue;
            } else if (char === ":") {
              // Scan backwards from ':' to find previous string key
              let keyEnd = cursor - 1;
              while (keyEnd >= 0 && /\s/.test(buffer[keyEnd]!)) keyEnd--;
              if (keyEnd >= 0 && buffer[keyEnd] === '"') {
                let keyStart = keyEnd - 1;
                while (keyStart >= 0 && buffer[keyStart] !== '"') keyStart--;
                if (keyStart >= 0) {
                  const keyName = buffer.slice(keyStart + 1, keyEnd);
                  currentPath.push(keyName);
                  if (
                    currentPath.join(".") === targetPath ||
                    keyName === targetPathParts[targetPathParts.length - 1]
                  ) {
                    // Next non-whitespace should be '['
                    // Let it continue to find '['
                  }
                }
              }
              cursor++;
              byteOffset++;
              continue;
            }
            cursor++;
            byteOffset++;
            continue;
          }

          // Now we are streaming inside the target array
          if (!inArray) {
            if (char === "[") {
              inArray = true;
            }
            cursor++;
            byteOffset++;
            continue;
          } else {
            // Inside array: looking for objects '{ ... }'
            if (char === "{") {
              if (objectDepth === 0) {
                objectStartIndex = cursor;
              }
              objectDepth++;
            } else if (char === "}") {
              objectDepth--;
              if (objectDepth === 0 && objectStartIndex !== -1) {
                // We have captured a complete JSON object!
                const objectStr = buffer.slice(objectStartIndex, cursor + 1);
                let parsedRow: unknown;
                try {
                  parsedRow = JSON.parse(objectStr);
                } catch (err) {
                  const parseErr = new ParseError(
                    `Malformed JSON object in array: ${(err as Error).message}`,
                    {
                      file: mergedOptions.filePath,
                      row: rowIndex + 1,
                      byteOffset,
                    }
                  );
                  errorHandler.handle(parseErr, {
                    file: mergedOptions.filePath,
                    row: rowIndex + 1,
                    byteOffset,
                    raw: objectStr,
                  });
                  if (mergedOptions.onParseError) {
                    mergedOptions.onParseError(err as Error, objectStr);
                  }
                  objectStartIndex = -1;
                  cursor++;
                  byteOffset++;
                  continue;
                }

                if (
                  typeof parsedRow === "object" &&
                  parsedRow !== null &&
                  !Array.isArray(parsedRow)
                ) {
                  rowsInCurrentBatch.push(parsedRow as Row);
                  rowIndex++;

                  if (rowsInCurrentBatch.length >= batchSize) {
                    yield {
                      rows: rowsInCurrentBatch,
                      offset: globalOffset,
                    };
                    globalOffset += rowsInCurrentBatch.length;
                    rowsInCurrentBatch = [];
                  }
                }

                objectStartIndex = -1;
              }
            } else if (char === "]" && objectDepth === 0) {
              // End of array reached
              inArray = false;
              break;
            }

            cursor++;
            byteOffset++;
          }
        }

        // Compact buffer while preserving active object if in the middle of parsing
        if (objectStartIndex !== -1) {
          buffer = buffer.slice(objectStartIndex);
          cursor = cursor - objectStartIndex;
          objectStartIndex = 0;
        } else {
          buffer = buffer.slice(cursor);
          cursor = 0;
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
      format: "JSON",
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
