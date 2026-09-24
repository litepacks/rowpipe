import type { DataBatch, DataStream, Row, TabularReader } from "../core/types.js";
import { InvalidArgumentError } from "../core/errors.js";

export interface HttpFetchOptions {
  headers?: Record<string, string>;
  bearer?: string;
  auth?: string;
  dataPath?: string;
  paginate?: "page" | "offset" | "cursor" | "none";
  pageParam?: string;
  offsetParam?: string;
  limitParam?: string;
  pageSize?: number;
  cursorParam?: string;
  cursorPath?: string;
  maxPages?: number;
  batchSize?: number;
}

function getNestedValue(obj: unknown, path: string): unknown {
  if (!obj || typeof obj !== "object") return undefined;
  const parts = path.split(".");
  let current: any = obj;
  for (const part of parts) {
    if (current === null || current === undefined) return undefined;
    current = current[part];
  }
  return current;
}

/**
 * Tabular stream reader fetching remote JSON/JSONL/CSV from HTTP/HTTPS endpoints with pagination support.
 */
export class HttpReader implements TabularReader {
  private url: string;
  private options: HttpFetchOptions;
  private batchSize: number;

  constructor(url: string, options: HttpFetchOptions = {}) {
    if (!url || typeof url !== "string") {
      throw new InvalidArgumentError("HttpReader requires a valid URL string");
    }
    this.url = url;
    this.options = options;
    this.batchSize = options.batchSize || 1000;
  }

  async *read(): DataStream {
    const headers: Record<string, string> = {
      Accept: "application/json, text/csv, application/x-ndjson, text/plain, */*",
      ...this.options.headers,
    };

    if (this.options.bearer) {
      headers["Authorization"] = `Bearer ${this.options.bearer}`;
    } else if (this.options.auth) {
      const b64 = Buffer.from(this.options.auth).toString("base64");
      headers["Authorization"] = `Basic ${b64}`;
    }

    const maxPages = this.options.maxPages || 100;
    const paginateMode = this.options.paginate || "none";
    const pageParam = this.options.pageParam || "page";
    const offsetParam = this.options.offsetParam || "offset";
    const limitParam = this.options.limitParam || "limit";
    const pageSize = this.options.pageSize || 100;

    let currentPage = 1;
    let currentOffset = 0;
    let nextCursor: string | undefined = undefined;
    let totalRowsEmitted = 0;

    let buffer: Row[] = [];

    for (let pageIdx = 0; pageIdx < maxPages; pageIdx++) {
      const targetUrl = new URL(this.url);

      if (paginateMode === "page") {
        targetUrl.searchParams.set(pageParam, String(currentPage));
        if (this.options.pageSize) {
          targetUrl.searchParams.set(limitParam, String(pageSize));
        }
      } else if (paginateMode === "offset") {
        targetUrl.searchParams.set(offsetParam, String(currentOffset));
        targetUrl.searchParams.set(limitParam, String(pageSize));
      } else if (paginateMode === "cursor" && nextCursor) {
        targetUrl.searchParams.set(this.options.cursorParam || "cursor", nextCursor);
      }

      const res = await fetch(targetUrl.toString(), {
        method: "GET",
        headers,
      });

      if (!res.ok) {
        throw new Error(`HTTP fetch failed with status ${res.status} (${res.statusText}) for ${targetUrl.toString()}`);
      }

      const contentType = res.headers.get("content-type") || "";
      const isJson = contentType.includes("json") || (!contentType.includes("csv") && !contentType.includes("text"));

      let pageRows: Row[] = [];

      if (isJson) {
        const jsonBody = await res.json();
        let records: unknown = jsonBody;

        if (this.options.dataPath) {
          records = getNestedValue(jsonBody, this.options.dataPath);
        } else if (!Array.isArray(jsonBody) && typeof jsonBody === "object" && jsonBody !== null) {
          // Auto-detect array field in envelope
          const obj = jsonBody as Record<string, unknown>;
          const arrayKey = Object.keys(obj).find((k) => Array.isArray(obj[k]));
          if (arrayKey) {
            records = obj[arrayKey];
          }
        }

        if (Array.isArray(records)) {
          pageRows = records.filter((r) => typeof r === "object" && r !== null) as Row[];
        } else if (typeof records === "object" && records !== null) {
          pageRows = [records as Row];
        }

        // Handle cursor extraction
        if (paginateMode === "cursor" && this.options.cursorPath) {
          const cursorVal = getNestedValue(jsonBody, this.options.cursorPath);
          nextCursor = typeof cursorVal === "string" || typeof cursorVal === "number" ? String(cursorVal) : undefined;
        }
      } else {
        // Plain text / CSV line streaming
        const text = await res.text();
        const lines = text.split("\n").map((l) => l.trim()).filter(Boolean);
        if (lines.length > 0) {
          const headers = lines[0]!.split(",").map((h) => h.trim().replace(/^["']|["']$/g, ""));
          for (let i = 1; i < lines.length; i++) {
            const values = lines[i]!.split(",");
            const row: Row = {};
            headers.forEach((h, idx) => {
              row[h] = values[idx] !== undefined ? values[idx]?.trim().replace(/^["']|["']$/g, "") : null;
            });
            pageRows.push(row);
          }
        }
      }

      if (pageRows.length === 0) {
        break;
      }

      for (const row of pageRows) {
        buffer.push(row);
        totalRowsEmitted++;

        if (buffer.length >= this.batchSize) {
          yield {
            rows: buffer,
            offset: totalRowsEmitted - buffer.length,
          };
          buffer = [];
        }
      }

      if (paginateMode === "none") {
        break;
      }

      currentPage++;
      currentOffset += pageRows.length;

      if (paginateMode === "cursor" && !nextCursor) {
        break;
      }
    }

    if (buffer.length > 0) {
      yield {
        rows: buffer,
        offset: totalRowsEmitted - buffer.length,
      };
    }
  }
}
