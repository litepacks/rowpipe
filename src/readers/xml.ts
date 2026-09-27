import { Readable } from "node:stream";
import { openDecompressedReadStream, isCompressedPath } from "../utils/compression.js";
import { ParseError } from "../core/errors.js";
import { RowErrorHandler } from "../core/error-handler.js";
import type {
  ColumnType,
  DataBatch,
  DataStream,
  InspectionMetadata,
  ReaderOptions,
  Row,
  TabularReader,
} from "../core/types.js";
import { flattenObject } from "../transforms/flatten.js";
import sax from "sax";

export interface XMLReaderOptions extends ReaderOptions {
  path?: string;
  attrPrefix?: string;
  flatten?: boolean;
  inferTypes?: boolean;
  filePath?: string;
}

/**
 * Coerces primitive text strings into numbers, booleans, or nulls when appropriate.
 */
export function coerceXmlPrimitive(text: string): unknown {
  if (text === "") return "";
  if (text === "true") return true;
  if (text === "false") return false;
  if (text === "null") return null;

  // Integer regex: preserve leading zeroes (e.g., zip codes "01234") as strings
  if (/^-?(?:0|[1-9]\d*)$/.test(text)) {
    const num = Number(text);
    if (Number.isSafeInteger(num)) return num;
  }

  // Float regex
  if (
    /^-?(?:0|[1-9]\d*)\.\d+(?:[eE][+-]?\d+)?$/.test(text) ||
    /^-?(?:0|[1-9]\d+)[eE][+-]?\d+$/.test(text)
  ) {
    const num = Number(text);
    if (Number.isFinite(num)) return num;
  }

  return text;
}

function tagMatches(actualTag: string, expectedTag: string): boolean {
  const actual = actualTag.toLowerCase();
  const expected = expectedTag.toLowerCase();
  if (actual === expected) return true;
  const colonIdx = actual.indexOf(":");
  if (colonIdx !== -1 && actual.slice(colonIdx + 1) === expected) {
    return true;
  }
  return false;
}

function parsePath(path?: string): string[] | null {
  if (!path) return null;
  const trimmed = path.trim().replace(/^[/]+/, "");
  if (!trimmed) return null;
  return trimmed.split(/[./]+/).map((s) => s.trim().toLowerCase()).filter(Boolean);
}

interface XmlNode {
  name: string;
  attributes: Record<string, string>;
  text: string;
  children: Record<string, any>;
  hasChildElements: boolean;
}

/**
 * High-performance, stream-first XML Reader.
 * Consumes arbitrarily large XML datasets event-by-event with bounded O(1) memory.
 * Does NOT construct an in-memory DOM.
 */
export class XMLReader implements TabularReader {
  private input: Readable | string;
  private options: XMLReaderOptions;

  constructor(input: Readable | string | any, options: XMLReaderOptions = {}) {
    if (
      typeof input === "object" &&
      input !== null &&
      !(input instanceof Readable) &&
      ("inputPath" in input || "input" in input)
    ) {
      this.input = input.inputPath || input.input || "-";
      this.options = { ...input, ...options };
    } else {
      this.input = input;
      this.options = { ...options };
    }
    if (typeof this.input === "string") {
      this.options.filePath = this.input;
    }
  }

  private getInputStream(opts?: ReaderOptions): Readable {
    return openDecompressedReadStream(this.input, { ...this.options, ...opts });
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: XMLReaderOptions = {
      ...this.options,
      ...options,
    };

    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);
    const attrPrefix = mergedOptions.attrPrefix !== undefined ? mergedOptions.attrPrefix : "@";
    const flatten = mergedOptions.flatten === true;
    const inferTypes = mergedOptions.inferTypes !== false;
    const pathSegments = parsePath(mergedOptions.path);

    const errorHandler = new RowErrorHandler({
      strategy: mergedOptions.onError,
      badRowsLog: mergedOptions.badRowsLog,
      sourceFile: mergedOptions.filePath,
    });

    const inputStream = this.getInputStream(mergedOptions);
    const saxStream = sax.createStream(true, {
      trim: true,
      xmlns: false,
    });

    const rowQueue: Row[] = [];
    let isStreamDone = false;
    let streamError: Error | null = null;
    let resolveNext: (() => void) | null = null;
    let rejectNext: ((err: Error) => void) | null = null;

    const notifyAvailable = () => {
      if (resolveNext) {
        const cb = resolveNext;
        resolveNext = null;
        rejectNext = null;
        cb();
      }
    };

    const notifyError = (err: Error) => {
      streamError = err;
      if (rejectNext) {
        const cb = rejectNext;
        resolveNext = null;
        rejectNext = null;
        cb(err);
      }
    };

    const tagStack: string[] = [];
    const nodeStack: XmlNode[] = [];
    let recordDepth = -1;

    const checkRecordMatch = (stack: string[]): boolean => {
      if (pathSegments) {
        if (pathSegments.length === 1) {
          return tagMatches(stack[stack.length - 1]!, pathSegments[0]!);
        }
        if (stack.length < pathSegments.length) return false;
        const slice = stack.slice(-pathSegments.length);
        for (let i = 0; i < pathSegments.length; i++) {
          if (!tagMatches(slice[i]!, pathSegments[i]!)) return false;
        }
        return true;
      }
      // Default: immediate child of root element
      return stack.length === 2;
    };

    saxStream.on("opentag", (node: any) => {
      const tagName = node.name;
      tagStack.push(tagName);

      if (recordDepth === -1 && checkRecordMatch(tagStack)) {
        recordDepth = tagStack.length;
      }

      if (recordDepth !== -1 && tagStack.length >= recordDepth) {
        const attributes: Record<string, string> = {};
        if (node.attributes) {
          for (const [k, v] of Object.entries(node.attributes)) {
            if (!k.toLowerCase().startsWith("xmlns")) {
              attributes[k] = String(v);
            }
          }
        }

        nodeStack.push({
          name: tagName,
          attributes,
          text: "",
          children: {},
          hasChildElements: false,
        });
      }
    });

    saxStream.on("text", (text: string) => {
      if (nodeStack.length > 0) {
        nodeStack[nodeStack.length - 1]!.text += text;
      }
    });

    saxStream.on("cdata", (cdata: string) => {
      if (nodeStack.length > 0) {
        nodeStack[nodeStack.length - 1]!.text += cdata;
      }
    });

    saxStream.on("closetag", (tagName: string) => {
      if (recordDepth !== -1 && tagStack.length >= recordDepth && nodeStack.length > 0) {
        const currentNode = nodeStack.pop()!;
        const trimmedText = currentNode.text.trim();
        const hasAttrs = Object.keys(currentNode.attributes).length > 0;
        let nodeValue: any;

        if (!currentNode.hasChildElements) {
          if (!hasAttrs) {
            nodeValue =
              trimmedText === ""
                ? null
                : inferTypes
                  ? coerceXmlPrimitive(trimmedText)
                  : trimmedText;
          } else {
            nodeValue = {};
            for (const [k, v] of Object.entries(currentNode.attributes)) {
              const attrKey = attrPrefix ? `${attrPrefix}${k}` : k;
              nodeValue[attrKey] = inferTypes ? coerceXmlPrimitive(v) : v;
            }
            if (trimmedText !== "") {
              nodeValue["#text"] = inferTypes ? coerceXmlPrimitive(trimmedText) : trimmedText;
            }
          }
        } else {
          nodeValue = { ...currentNode.children };
          for (const [k, v] of Object.entries(currentNode.attributes)) {
            const attrKey = attrPrefix ? `${attrPrefix}${k}` : k;
            nodeValue[attrKey] = inferTypes ? coerceXmlPrimitive(v) : v;
          }
          if (trimmedText !== "") {
            nodeValue["#text"] = inferTypes ? coerceXmlPrimitive(trimmedText) : trimmedText;
          }
        }

        if (tagStack.length === recordDepth) {
          // Closed record element
          let row: Row;
          if (typeof nodeValue === "object" && nodeValue !== null && !Array.isArray(nodeValue)) {
            row = nodeValue;
          } else {
            const leafName = tagName.includes(":") ? tagName.split(":")[1]! : tagName;
            row = { [leafName]: nodeValue };
          }

          if (flatten) {
            row = flattenObject(row);
          }

          rowQueue.push(row);
          if (rowQueue.length >= batchSize * 2) {
            inputStream.pause();
          }
          notifyAvailable();
          recordDepth = -1;
        } else if (nodeStack.length > 0) {
          // Child of parent node
          const parent = nodeStack[nodeStack.length - 1]!;
          parent.hasChildElements = true;
          const childKey = tagName.includes(":") ? tagName.split(":")[1]! : tagName;
          if (childKey in parent.children) {
            const existing = parent.children[childKey];
            if (Array.isArray(existing)) {
              existing.push(nodeValue);
            } else {
              parent.children[childKey] = [existing, nodeValue];
            }
          } else {
            parent.children[childKey] = nodeValue;
          }
        }
      }

      tagStack.pop();
    });

    saxStream.on("error", (err: any) => {
      const parseError = new ParseError(`XML parse error: ${err.message}`, {
        file: mergedOptions.filePath,
      });

      if (mergedOptions.onError === "skip" || mergedOptions.onError === "log") {
        errorHandler.handle(parseError, {
          file: mergedOptions.filePath,
          raw: err.message,
        });
        try {
          (saxStream as any)._parser.error = null;
          (saxStream as any)._parser.resume();
        } catch {
          // Ignore
        }
      } else {
        notifyError(parseError);
      }
    });

    saxStream.on("end", () => {
      isStreamDone = true;
      notifyAvailable();
    });

    inputStream.on("error", (err) => {
      notifyError(err);
    });

    inputStream.pipe(saxStream);

    let globalOffset = 0;
    try {
      while (true) {
        if (rowQueue.length > 0) {
          const count = Math.min(rowQueue.length, batchSize);
          const chunk = rowQueue.splice(0, count);
          if (inputStream.isPaused() && rowQueue.length < batchSize) {
            inputStream.resume();
          }

          if (chunk.length > 0) {
            yield {
              rows: chunk,
              offset: globalOffset,
            };
            globalOffset += chunk.length;
          }
          continue;
        }

        if (streamError) {
          throw streamError;
        }

        if (isStreamDone && rowQueue.length === 0) {
          break;
        }

        await new Promise<void>((resolve, reject) => {
          resolveNext = resolve;
          rejectNext = reject;
        });
      }
    } finally {
      errorHandler.close();
      if (!inputStream.destroyed) {
        inputStream.destroy();
      }
    }
  }

  async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    let rowCount = 0;
    let columnNames: string[] = [];
    const sampleRows: Row[] = [];

    for await (const batch of this.read({ ...options, batchSize: 500 })) {
      for (const row of batch.rows) {
        rowCount++;
        if (sampleRows.length < 50) {
          sampleRows.push(row);
          for (const k of Object.keys(row)) {
            if (!columnNames.includes(k)) {
              columnNames.push(k);
            }
          }
        }
      }
    }

    const columns = columnNames.map((name) => {
      let nonNullCount = 0;
      let detectedType: ColumnType = "string";

      for (const r of sampleRows) {
        const val = r[name];
        if (val !== null && val !== undefined) {
          nonNullCount++;
          if (typeof val === "number") {
            detectedType = Number.isInteger(val) ? "integer" : "number";
          } else if (typeof val === "boolean") {
            detectedType = "boolean";
          } else if (typeof val === "object") {
            detectedType = "json";
          }
        }
      }

      const nullPercentage =
        sampleRows.length > 0
          ? Math.round(((sampleRows.length - nonNullCount) / sampleRows.length) * 100)
          : 0;

      return {
        name,
        type: detectedType,
        nullPercentage,
      };
    });

    return {
      format: "xml",
      rowCount,
      columnCount: columns.length,
      columns,
    };
  }
}
