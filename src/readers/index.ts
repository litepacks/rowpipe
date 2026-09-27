import type { Readable } from "node:stream";
import { InvalidArgumentError } from "../core/errors.js";
import type { FormatAdapter, ReaderOptions, TabularReader } from "../core/types.js";
import { stripCompressionExtension } from "../utils/compression.js";
import { FileSystemReader } from "../files/reader.js";
import { CSVReader } from "./csv.js";
import { JSONReader } from "./json.js";
import { JSONLReader } from "./jsonl.js";
import { ParquetReader } from "./parquet.js";
import { XLSXReader } from "./xlsx.js";
import { ArrowReader, FeatherReader } from "./arrow.js";
import { AvroReader } from "./avro.js";
import { XMLReader } from "./xml.js";

import { isDatabaseUrl } from "../db/url.js";
import { DatabaseReader } from "../db/source.js";
import { MultiFileReader, isGlobPattern } from "./multi-file.js";
import { sniffFormatFromFile } from "./sniff.js";
import { findClosestMatch } from "../utils/fuzzy.js";

export * from "../files/reader.js";
export * from "../db/source.js";
export * from "./multi-file.js";
export * from "./sniff.js";
export * from "./csv.js";
export * from "./json.js";
export * from "./jsonl.js";
export * from "./parquet.js";
export * from "./xlsx.js";
export * from "./arrow.js";
export * from "./avro.js";
export * from "./xml.js";

const adapters: Record<string, FormatAdapter> = {
  csv: {
    name: "CSV",
    extensions: [".csv", ".txt"],
    createReader: (input, options) => new CSVReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  tsv: {
    name: "TSV",
    extensions: [".tsv", ".tab"],
    createReader: (input, options) =>
      new CSVReader(input, { ...options, delimiter: options?.delimiter ?? "\t" }),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  psv: {
    name: "PSV",
    extensions: [".psv"],
    createReader: (input, options) =>
      new CSVReader(input, { ...options, delimiter: options?.delimiter ?? "|" }),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  parquet: {
    name: "Parquet",
    extensions: [".parquet", ".pq"],
    createReader: (input, options) => new ParquetReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  arrow: {
    name: "Arrow",
    extensions: [".arrow"],
    createReader: (input, options) => new ArrowReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  feather: {
    name: "Feather",
    extensions: [".feather"],
    createReader: (input, options) => new ArrowReader(input, { ...options, format: "feather" }),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  avro: {
    name: "Avro",
    extensions: [".avro"],
    createReader: (input, options) => new AvroReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  xml: {
    name: "XML",
    extensions: [".xml"],
    createReader: (input, options) => new XMLReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  json: {
    name: "JSON",
    extensions: [".json"],
    createReader: (input, options) => new JSONReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  jsonl: {
    name: "JSONL",
    extensions: [".jsonl", ".ldjson"],
    createReader: (input, options) => new JSONLReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  ndjson: {
    name: "NDJSON",
    extensions: [".ndjson"],
    createReader: (input, options) => new JSONLReader(input, { ...options, format: "ndjson" }),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  xlsx: {
    name: "XLSX",
    extensions: [".xlsx", ".xlsm"],
    createReader: (input, options) => new XLSXReader(input, options),
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  files: {
    name: "Filesystem",
    extensions: [],
    createReader: (input, options) => {
      if (typeof input !== "string") {
        throw new InvalidArgumentError("Filesystem reader requires a string path");
      }
      return new FileSystemReader({ root: input, ...options });
    },
    createWriter: () => {
      throw new Error("Filesystem is a read-only source adapter");
    },
  },
  filesystem: {
    name: "Filesystem",
    extensions: [],
    createReader: (input, options) => {
      if (typeof input !== "string") {
        throw new InvalidArgumentError("Filesystem reader requires a string path");
      }
      return new FileSystemReader({ root: input, ...options });
    },
    createWriter: () => {
      throw new Error("Filesystem is a read-only source adapter");
    },
  },
  db: {
    name: "Database",
    extensions: [".db", ".sqlite", ".sqlite3"],
    createReader: (input, options) => {
      if (typeof input !== "string") {
        throw new InvalidArgumentError("Database reader requires a connection URL or file path");
      }
      return new DatabaseReader(input, options);
    },
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
  database: {
    name: "Database",
    extensions: [],
    createReader: (input, options) => {
      if (typeof input !== "string") {
        throw new InvalidArgumentError("Database reader requires a connection URL or file path");
      }
      return new DatabaseReader(input, options);
    },
    createWriter: () => {
      throw new Error("Writer adapter called from reader");
    },
  },
};

/**
 * Infers format name from file path or extension, with fallback to magic byte sniffing.
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

  // Fallback to magic byte sniffing if file exists on disk
  if (filePath !== "-") {
    const sniffed = sniffFormatFromFile(filePath);
    if (sniffed) {
      return sniffed;
    }
  }

  return null;
}

/**
 * Creates an appropriate TabularReader for input and format.
 */
export function createReader(
  input: Readable | string | string[],
  options: ReaderOptions & { format?: string; addFilename?: boolean; fileCol?: string } = {}
): TabularReader {
  if (Array.isArray(input)) {
    return new MultiFileReader(input, options);
  }

  let format = options.format?.toLowerCase();

  if (!format && typeof input === "string" && input !== "-") {
    if (isDatabaseUrl(input)) {
      return new DatabaseReader(input, options);
    }
    if (isGlobPattern(input)) {
      return new MultiFileReader(input, options);
    }
    format = inferFormatFromPath(input) ?? undefined;
  }

  if (!format) {
    format = "csv";
  }

  const adapter = adapters[format];
  if (!adapter) {
    const suggestion = findClosestMatch(format, Object.keys(adapters));
    const didYouMean = suggestion ? ` Did you mean "${suggestion}"?` : "";
    throw new InvalidArgumentError(
      `Unsupported input format: "${format}".${didYouMean} Supported formats are: ${Object.keys(adapters).join(", ")}`
    );
  }

  return adapter.createReader(input, options);
}

