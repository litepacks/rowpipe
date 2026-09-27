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
import { ArrowWriter, FeatherWriter } from "./arrow.js";
import { AvroWriter } from "./avro.js";
import { XMLWriter } from "./xml.js";

import { isDatabaseUrl } from "../db/url.js";
import { DatabaseWriter } from "../db/sink.js";
import { findClosestMatch } from "../utils/fuzzy.js";

export * from "../db/sink.js";
export * from "./csv.js";
export * from "./json.js";
export * from "./jsonl.js";
export * from "./markdown.js";
export * from "./parquet.js";
export * from "./xlsx.js";
export * from "./table.js";
export * from "./arrow.js";
export * from "./avro.js";
export * from "./xml.js";

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
  arrow: {
    name: "Arrow",
    extensions: [".arrow"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new ArrowWriter(output, options),
  },
  feather: {
    name: "Feather",
    extensions: [".feather"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new ArrowWriter(output, { ...options, format: "feather" }),
  },
  avro: {
    name: "Avro",
    extensions: [".avro"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new AvroWriter(output, options),
  },
  xml: {
    name: "XML",
    extensions: [".xml"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new XMLWriter(output, options),
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
    extensions: [".jsonl", ".ldjson"],
    createReader: () => {
      throw new Error("Reader adapter called from writer");
    },
    createWriter: (output, options) => new JSONLWriter(output),
  },
  ndjson: {
    name: "NDJSON",
    extensions: [".ndjson"],
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
      return new DatabaseWriter(output, {
        table: options?.table || "",
        ...options,
        dryRun: options?.dryRun ? Boolean(options.dryRun) : undefined,
      });
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
      return new DatabaseWriter(output, {
        table: options?.table || "",
        ...options,
        dryRun: options?.dryRun ? Boolean(options.dryRun) : undefined,
      });
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
      return new DatabaseWriter(output, {
        table: options?.table || "",
        ...options,
        dryRun: options?.dryRun ? Boolean(options.dryRun) : undefined,
      });
    }
    format = inferFormatFromPath(output) ?? undefined;
  }

  if (!format) {
    format = "csv";
  }

  const adapter = adapters[format];
  if (!adapter) {
    const suggestion = findClosestMatch(format, Object.keys(adapters));
    const didYouMean = suggestion ? ` Did you mean "${suggestion}"?` : "";
    throw new InvalidArgumentError(
      `Unsupported output format: "${format}".${didYouMean} Supported formats are: ${Object.keys(adapters).join(", ")}`
    );
  }

  return adapter.createWriter(output, options);
}
