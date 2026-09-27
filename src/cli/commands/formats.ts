import { formatTable } from "../../writers/table.js";
import type { Row } from "../../core/types.js";

export interface FormatCapability {
  format: string;
  name: string;
  read: "Streaming" | "Full" | "No";
  write: "Streaming" | "Full" | "No";
  compression: string;
  schema: string;
  nested: string;
  category: "Columnar" | "Binary" | "Text" | "Database" | "Presentation";
}

export const SUPPORTED_FORMATS: FormatCapability[] = [
  {
    format: "csv",
    name: "Comma-Separated Values",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "No",
    category: "Text",
  },
  {
    format: "tsv",
    name: "Tab-Separated Values",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "No",
    category: "Text",
  },
  {
    format: "psv",
    name: "Pipe-Separated Values",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "No",
    category: "Text",
  },
  {
    format: "json",
    name: "JSON Object Array",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "Yes (flatten/explode)",
    category: "Text",
  },
  {
    format: "jsonl",
    name: "JSON Lines",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "Yes (flatten/explode)",
    category: "Text",
  },
  {
    format: "ndjson",
    name: "Newline Delimited JSON",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "Yes (flatten/explode)",
    category: "Text",
  },
  {
    format: "arrow",
    name: "Apache Arrow IPC / Feather",
    read: "Streaming",
    write: "Streaming",
    compression: "LZ4, ZSTD",
    schema: "Native Arrow Schema",
    nested: "Yes (Struct/List)",
    category: "Columnar",
  },
  {
    format: "parquet",
    name: "Apache Parquet",
    read: "Streaming",
    write: "Streaming",
    compression: "Snappy, Gzip, ZSTD",
    schema: "Native Parquet Schema",
    nested: "Yes (Struct/List)",
    category: "Columnar",
  },
  {
    format: "avro",
    name: "Apache Avro (OCF)",
    read: "Streaming",
    write: "Streaming",
    compression: "Deflate, Snappy",
    schema: "Native Avro Schema",
    nested: "Yes (Record/Array)",
    category: "Binary",
  },
  {
    format: "xml",
    name: "Extensible Markup Language",
    read: "Streaming",
    write: "Streaming",
    compression: "Gzip, ZSTD, Deflate",
    schema: "Inferred / Cast",
    nested: "Yes (--path tag selector)",
    category: "Text",
  },
  {
    format: "xlsx",
    name: "Microsoft Excel Workbook",
    read: "Streaming",
    write: "Full",
    compression: "ZIP (Native)",
    schema: "Inferred / Cast",
    nested: "No",
    category: "Binary",
  },
  {
    format: "markdown",
    name: "GitHub Flavored Markdown",
    read: "No",
    write: "Streaming",
    compression: "None",
    schema: "Display",
    nested: "No",
    category: "Presentation",
  },
  {
    format: "table",
    name: "Unicode / ASCII Terminal Table",
    read: "No",
    write: "Streaming",
    compression: "None",
    schema: "Display",
    nested: "No",
    category: "Presentation",
  },
  {
    format: "sqlite",
    name: "SQLite Database",
    read: "Streaming",
    write: "Streaming",
    compression: "None",
    schema: "SQLite Table DDL",
    nested: "JSON column",
    category: "Database",
  },
  {
    format: "postgres",
    name: "PostgreSQL Database",
    read: "Streaming",
    write: "Streaming",
    compression: "Wire (TLS/GSS)",
    schema: "Postgres Table DDL",
    nested: "JSON / JSONB",
    category: "Database",
  },
  {
    format: "mysql",
    name: "MySQL Database",
    read: "Streaming",
    write: "Streaming",
    compression: "Wire (TLS)",
    schema: "MySQL Table DDL",
    nested: "JSON column",
    category: "Database",
  },
];

export interface FormatsCommandOptions {
  json?: boolean;
  markdown?: boolean;
  category?: string;
}

/**
 * CLI command handler for `rowpipe formats`.
 */
export async function formatsCommand(options: FormatsCommandOptions = {}): Promise<void> {
  let list = SUPPORTED_FORMATS;

  if (options.category) {
    const filterCat = options.category.toLowerCase();
    list = list.filter((f) => f.category.toLowerCase() === filterCat);
  }

  if (options.json) {
    process.stdout.write(JSON.stringify(list, null, 2) + "\n");
    return;
  }

  if (options.markdown) {
    const header = "| Format | Name | Category | Read | Write | Compression | Schema | Nested |";
    const divider = "|:---|:---|:---|:---|:---|:---|:---|:---|";
    const lines = list.map(
      (f) =>
        `| **${f.format}** | ${f.name} | ${f.category} | ${f.read} | ${f.write} | ${f.compression} | ${f.schema} | ${f.nested} |`
    );
    process.stdout.write([header, divider, ...lines].join("\n") + "\n");
    return;
  }

  const rows: Row[] = list.map((f) => ({
    format: f.format,
    name: f.name,
    category: f.category,
    read: f.read,
    write: f.write,
    compression: f.compression,
    schema: f.schema,
    nested: f.nested,
  }));

  const output = formatTable(rows, { style: "unicode" });
  process.stdout.write(output);
}
