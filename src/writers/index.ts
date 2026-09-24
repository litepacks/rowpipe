import type { Writable } from "node:stream";
import { InvalidArgumentError } from "../core/errors.js";
import type { FormatAdapter, TabularWriter, WriterOptions } from "../core/types.js";
import { stripCompressionExtension } from "../utils/compression.js";
import { CSVWriter } from "./csv.js";
import { JSONWriter } from "./json.js";
import { JSONLWriter } from "./jsonl.js";
import { MarkdownWriter } from "./markdown.js";
import { ParquetWriter } from "./parquet.js";
import { XLSXWriter } from "./xlsx.js";
import { TableWriter } from "./table.js";

import { isDatabaseUrl } from "../db/url.js";
import { DatabaseWriter } from "../db/sink.js";

export * from "../db/sink.js";
export * from "./csv.js";
export * from "./json.js";
export * from "./jsonl.js";
export * from "./markdown.js";
export * from "./parquet.js";
export * from "./xlsx.js";
export * from "./table.js";

const adapters: Record<string, FormatAdapter> = {
  table: {
    name: "Table",
    extensions: [],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new TableWriter(output, options as any),
  },
  csv: {
    name: "CSV",
    extensions: [".csv", ".txt"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new CSVWriter(output, options),
  },
  tsv: {
    name: "TSV",
    extensions: [".tsv", ".tab"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) =>
      new CSVWriter(output, { ...options, delimiter: options?.delimiter ?? "\t" }),
  },
  psv: {
    name: "PSV",
    extensions: [".psv"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) =>
      new CSVWriter(output, { ...options, delimiter: options?.delimiter ?? "|" }),
  },
  markdown: {
    name: "Markdown",
    extensions: [".md", ".markdown"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new MarkdownWriter(output, options),
  },
  md: {
    name: "Markdown",
    extensions: [".md"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new MarkdownWriter(output, options),
  },
  parquet: {
    name: "Parquet",
    extensions: [".parquet", ".pq"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new ParquetWriter(output, options),
  },
  json: {
    name: "JSON",
    extensions: [".json"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new JSONWriter(output),
  },
  jsonl: {
    name: "JSONL",
    extensions: [".jsonl", ".ndjson", ".ldjson"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new JSONLWriter(output),
  },
  xlsx: {
    name: "XLSX",
    extensions: [".xlsx"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new XLSXWriter(output, options),
  },
  db: {
    name: "Database",
    extensions: [".db", ".sqlite", ".sqlite3"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => {
      if (typeof output !== "string") {
        throw new InvalidArgumentError("Database writer requires a connection URL or file path");
      }
      return new DatabaseWriter(output, { table: options?.table || "", ...options });
    },
  },
  database: {
    name: "Database",
    extensions: [],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => {
      if (typeof output !== "string") {
        throw new InvalidArgumentError("Database writer requires a connection URL or file path");
      }
      return new DatabaseWriter(output, { table: options?.table || "", ...options });
    },
  },
};

/**
 * Infers format name from file path or extension.
 */
export function inferFormatFromPath(filePath: string): string | null {
  if (isDatabaseUrl(filePath)) {
    return "db";
  }
  const cleanPath = stripCompressionExtension(filePath.toLowerCase());
  for (const [format, adapter] of Object.entries(adapters)) {
    if (adapter.extensions.some((ext) => cleanPath.endsWith(ext))) {
      return format;
    }
  }
  return null;
}

/**
 * Creates an appropriate TabularWriter for output and format.
 */
export function createWriter(
  output: Writable | string,
  options: WriterOptions & { format?: string } = {}
): TabularWriter {
  let format = options.format?.toLowerCase();

  if (!format && typeof output === "string" && output !== "-") {
    if (isDatabaseUrl(output)) {
      return new DatabaseWriter(output, { table: options?.table || "", ...options });
    }
    format = inferFormatFromPath(output) ?? undefined;
  }

  if (!format) {
    format = "csv";
  }

  const adapter = adapters[format];
  if (!adapter) {
    throw new InvalidArgumentError(
      `Unsupported output format: "${format}". Supported formats are: ${Object.keys(adapters).join(", ")}`
    );
  }

  return adapter.createWriter(output, options);
}
