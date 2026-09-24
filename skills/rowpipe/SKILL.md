---
name: rowpipe
description: Stream-first tabular data toolkit for inspecting, analyzing, transforming, validating, converting, mapping, reducing, and streaming large datasets across PostgreSQL, MySQL, SQLite, CSV, TSV, JSON, JSONL, XLSX, Apache Parquet, and Markdown formats with bounded O(1) memory and extreme throughput (~500,000 rows/s).
---

# Rowpipe: Stream-First Tabular Data Toolkit

Rowpipe is a high-performance, stream-first command-line toolkit and Node.js library for tabular data processing. It processes gigabyte-scale datasets with bounded $O(1)$ memory usage, backpressure, batching, pushdown optimization, and zero-`eval` safe expression execution.

---

## 🚀 Quick Command Reference

| Task | Command Syntax | Description |
|---|---|---|
| **Database** | `rowpipe db <url> [--table <table>] [--query <sql>]` | Stream directly from PostgreSQL, MySQL, and SQLite databases with cursor batching and pushdown |
| **DB Tables** | `rowpipe db <url> --tables [--json]` | List all tables in the target database |
| **DB Schema** | `rowpipe db <url> --schema <table> [--json]` | Inspect database table columns, types, nullability, and primary keys |
| **File to DB** | `rowpipe <file> --to-db <url> --to-table <table> [--create-table]` | Stream load files directly into database tables with batch inserts and transactions |
| **Inspect** | `rowpipe inspect <file> [--sheet <name>] [--json]` | Inspect row count, column types, null %, and HLL uniqueness |
| **Convert** | `rowpipe convert <input> <output> [--all-sheets]` | Stream convert between CSV, TSV, JSON, JSONL, XLSX, Parquet, Markdown |
| **Schema** | `rowpipe schema <file> [--full] [--sample <n>]` | Infer column types and semantic patterns (email, url, uuid) |
| **Stats** | `rowpipe stats <file> [--column <name>]` | Streaming Welford numeric stats & HyperLogLog distinct count |
| **Map** | `rowpipe map <file> "<col1=expr1>" "<col2=expr2>"` | Derive and compute columns per row with arithmetic/JSON dot-notation |
| **Filter** | `rowpipe filter <file> "<expression>"` | Filter rows using safe expression engine and pipe syntax (`\|`) |
| **Reduce** | `rowpipe reduce <file> "<agg1=func(col)>" [--by <cols>]` | Global & group-by streaming aggregations (`sum`, `avg`, `min`, `max`, `count`, `countDistinct`, `stddev`) |
| **Limit / Head** | `rowpipe limit <file> <n>` / `rowpipe head <file> [-n <n>]` | Emit first N rows with early upstream stream cancellation |
| **Offset** | `rowpipe offset <file> <n>` | Skip first N rows without scanning remainder |
| **Tail** | `rowpipe tail <file> [-n <n>]` | Emit last N rows using bounded $O(N)$ circular ring buffer |
| **Sort** | `rowpipe sort <file> --by <cols> [--memory-limit <m>]` | Multi-column typed external merge sort with disk run spilling and $K$-way min-heap |
| **Top** | `rowpipe top <file> --by <cols> [-n <n>]` | Extract Top-K largest/smallest records using bounded $O(K)$ heap |
| **Unique** | `rowpipe unique <file> [--by <cols>] [--keep first\|last]` | Deduplicate rows by key columns or entire rows with spillable hash key store |
| **Count** | `rowpipe count <file> [--by <cols>] [--distinct <col>]` | Ultra-fast row total, distinct count (HyperLogLog), or group-count |
| **Group** | `rowpipe group <file> --by <cols> [--count] [--sum <c>]` | Group tabular streams with hash accumulation and multi-column aggregations |
| **Explain** | `rowpipe explain <file\|db_url> [pipeline flags]` | Visualize execution pipeline, database pushdown, and memory bounds |
| **Select** | `rowpipe select <file> <col1,col2,...>` | Project subset of columns in order |
| **Rename** | `rowpipe rename <file> <old1=new1> <old2=new2>` | Rename columns in stream |
| **Cast** | `rowpipe cast <file> <col1:type> [--on-error null]` | Stream cast types (`integer`, `number`, `boolean`, `date`) |
| **Sample** | `rowpipe sample <file> --rows <n> [--seed <s>]` | Reservoir sampling with bounded memory |
| **Validate** | `rowpipe validate <file> --schema <schema.json>` | Validate stream against JSON schema definition |
| **Diff** | `rowpipe diff <left> <right> --key <cols> [opts]` | Stream compare datasets by key across formats and databases with bounded RAM |
| **Files** | `rowpipe files <path> [--include <glob>] [opts]` | Stream filesystem directory as tabular records with bounded memory, globs, hashes, and MIME |

---

## 🛠️ Common Workflows & Examples

### 1. Inspect & Schema Inference (CSV, TSV, Parquet, Excel)
```bash
# Inspect CSV / TSV / JSONL file
rowpipe inspect dataset.csv
rowpipe inspect data.tsv

# Inspect Apache Parquet file directly
rowpipe inspect dataset.parquet --json

# Inspect multi-sheet Excel workbook
rowpipe inspect workbook.xlsx
rowpipe inspect workbook.xlsx --sheet Orders

# Full schema inference with semantic types (email, URL, UUID, date)
rowpipe schema huge.csv --full
```

### 2. Format Conversion, Parquet & Gzip Streams
```bash
# Convert CSV to Apache Parquet
rowpipe convert sales.csv sales.parquet

# Convert Parquet to compressed JSONL (.jsonl.gz)
rowpipe convert sales.parquet sales.jsonl.gz

# Filter Parquet and output directly as GitHub Markdown table
rowpipe filter dataset.parquet "amount > 500" --to markdown

# Convert specific Excel worksheet to TSV
rowpipe convert workbook.xlsx orders.tsv --sheet Orders

# Bulk export all worksheets in workbook to a directory
rowpipe convert workbook.xlsx --all-sheets --out-dir ./exported_sheets --to csv
```

### 3. Row Derivations & Calculations (`map`)
Derive calculated columns using arithmetic, strings, dates, and direct JSON dot-notation:
```bash
# Arithmetic & percentage formulas
rowpipe map sales.csv \
  "profit = revenue - cost" \
  "tax = revenue * 0.20" \
  "margin = ((revenue - cost) / revenue) * 100"

# Direct JSON dot-notation & pipe transformations
rowpipe map events.jsonl \
  "city = payload.user.address.city" \
  "email = payload.user.email | lower | trim" \
  "item_price = payload.items.0.price" \
  "year = created_at | year"

# Conditional mapping
rowpipe map orders.csv "net_total = total | if(is_vip, total * 0.90, total) | round(2)"
```

### 4. Advanced Row Filtering (`filter`)
```bash
# Arithmetic & comparison on Parquet / CSV
rowpipe filter sales.parquet '(revenue - cost) / revenue >= 0.25'

# Pipe syntax for string & date functions
rowpipe filter users.csv 'email | lower | endsWith("@corp.com")'
rowpipe filter orders.csv 'created_at | year == 2026 && total > 500'

# Direct JSON dot-notation in filter
rowpipe filter events.jsonl.gz 'payload.user.city == "Istanbul" && payload.items.0.price > 50'

# Set membership and range checks
rowpipe filter products.tsv 'category | in("Electronics", "Computers") && price | between(100, 1000)'
```

### 5. Group-By & Global Aggregations (`reduce`)
```bash
# 1. Global summary (Single row output)
rowpipe reduce sales.parquet \
  "total_revenue = sum(revenue)" \
  "avg_margin = avg(margin)" \
  "min_price = min(price)" \
  "max_price = max(price)" \
  "total_orders = count()" \
  "unique_customers = countDistinct(customer_id)"

# 2. Group-By Streaming Aggregations
rowpipe reduce sales.csv \
  "total_revenue = sum(revenue)" \
  "avg_profit = avg(profit)" \
  "order_count = count()" \
  --by country,category
```

Supported reduce functions:
- `sum(col)` — Numeric sum
- `avg(col)` / `mean(col)` — Average
- `min(col)` / `max(col)` — Minimum / Maximum
- `count()` / `count(col)` — Row / non-null count
- `countDistinct(col)` — HyperLogLog distinct cardinality ($O(1)$ memory)
- `stddev(col)` / `variance(col)` — Welford single-pass online standard deviation / variance
- `first(col)` / `last(col)` — First non-null / last value

### 6. End-to-End Unix Pipe Processing Chain
```bash
cat transactions.csv.gz | \
  rowpipe map - "profit=revenue - cost" "margin=((revenue - cost) / revenue) * 100" | \
  rowpipe filter - "margin >= 15" | \
  rowpipe reduce - "total_profit=sum(profit)" "avg_margin=avg(margin)" "count=count()" --by country | \
  rowpipe convert - --to markdown
```

### 7. Dataset Diffing & Change Feeds (`diff`)
Compare two datasets by key across formats with bounded memory ($O(1)$) and automatic disk spilling:

```bash
# 1. Basic Keyed Diff
rowpipe diff yesterday.csv today.csv --key id

# 2. Composite Key & Cross-Format (CSV vs Parquet)
rowpipe diff old.csv new.parquet --key country,user_id

# 3. Multi-Sheet Excel Workbook Diff
rowpipe diff old.xlsx new.xlsx --sheet Users --key id

# 4. Filter Columns & String Normalization
rowpipe diff old.csv new.csv \
  --key id \
  --ignore updated_at,last_seen \
  --trim \
  --ignore-case \
  --epsilon 0.001

# 5. Output Modes: Rows, JSONL Patch, and JSON
rowpipe diff old.csv new.csv --key id --format rows --only changed --limit 50
rowpipe diff old.csv new.csv --key id --format patch > changes.jsonl
rowpipe diff old.csv new.csv --key id --json

# 6. CI/CD Validation (Fails with exit code 1 if differences found)
rowpipe diff expected.csv actual.csv --key id --fail-on-diff
```

### 8. Filesystem as Streaming Tabular Records (`files`) & Pretty Bytes
Stream files and directory trees with bounded memory, globs, streaming hashes, and MIME detection:

```bash
# 1. Stream directory records with human-readable sizes (-h / --human)
rowpipe files ./src -h --include "**/*.ts"

# 2. Filter files by human byte string (parsebytes) and derive pretty size (formatbytes)
rowpipe files ./uploads | \
  rowpipe filter - 'size > parsebytes("10MB")' | \
  rowpipe map - 'size_h = formatbytes(size)' | \
  rowpipe select - relative_path,size_h

# 3. Group by extension with pretty total size in Markdown table
rowpipe files . | \
  rowpipe reduce - 'count=count()' 'total_bytes=sum(size)' --by extension | \
  rowpipe map - 'size_pretty=formatbytes(total_bytes)' | \
  rowpipe convert - --to markdown

# 4. Stream SHA-256 or fast hashes
rowpipe files ./assets --hash sha256 --to jsonl
rowpipe files ./dataset --hash fast

# 5. Generate deterministic build snapshot
rowpipe files snapshot ./dist --hash sha256 --to jsonl > dist.snapshot.jsonl

# 6. Direct directory diffing
rowpipe files diff ./dist-v1 ./dist-v2 --hash sha256
```

---

## 💻 Node.js Library API

```typescript
import {
  createPipeline,
  createReader,
  createWriter,
  CSVReader,
  JSONLWriter,
  ParquetReader,
  ParquetWriter,
  MarkdownWriter,
  XLSXReader,
  FileSystemReader,
  mapRows,
  filterRows,
  reduceRows,
  diffRows,
  computeDiff,
} from "rowpipe";
import { createReadStream, createWriteStream } from "node:fs";

// 1. Streaming Parquet -> Filter -> Markdown Pipeline
const reader = new ParquetReader("sales.parquet");
const writer = new MarkdownWriter(createWriteStream("summary.md"));

await createPipeline(reader)
  .pipe(mapRows({
    profit: "revenue - cost",
    margin: "((revenue - cost) / revenue) * 100",
  }))
  .pipe(filterRows("margin >= 15"))
  .pipe(reduceRows({
    by: ["country"],
    aggregations: [
      "total_profit = sum(profit)",
      "avg_margin = avg(margin)",
      "count = count()",
    ],
  }))
  .to(writer);

// 2. Stream Filesystem Directory as Tabular Rows
const fsReader = new FileSystemReader({
  root: "./src",
  include: ["**/*.ts"],
  hash: "sha256",
  mime: true,
});

for await (const batch of fsReader.read()) {
  for (const file of batch.rows) {
    console.log(`${file.relative_path} (${file.size} bytes) - Hash: ${file.hash}`);
  }
}

// 3. Streaming Diff Events
for await (const event of diffRows({
  leftPath: "old.csv",
  rightPath: "new.parquet",
  keys: ["id"],
  ignore: ["updated_at"],
})) {
  if (event.type === "changed") {
    console.log(`Row ${JSON.stringify(event.key)} changed:`, event.changes);
  }
}
```
