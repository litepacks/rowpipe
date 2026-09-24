# Rowpipe

> **A stream-first tabular data toolkit for CSV, TSV, JSON, JSONL, XLSX, Apache Parquet, and Markdown.**
>
> *Don't load the dataset. Stream through it.*

Rowpipe is a high-performance, bounded-memory command-line toolkit and Node.js library for reading, inspecting, analyzing, transforming, validating, and converting tabular datasets across CSV, TSV, PSV, JSON, JSONL, XLSX, Apache Parquet (`.parquet`), Markdown tables (`.md`), and transparent Gzip (`.gz`) streams.

Designed around stream backpressure and batch processing, Rowpipe processes multi-gigabyte and multi-million-row datasets with constant $O(1)$ memory usage.

---

## Highlights

- **Stream-First Architecture**: Datasets are never buffered entirely in memory; rows flow in configurable batches through async generator pipelines.
- **Rich Format Ecosystem**: Native streaming support for **PostgreSQL, MySQL, SQLite, CSV, TSV / PSV, JSON, JSONL, XLSX, Apache Parquet (`.parquet`), Markdown tables (`.md`), and Gzip (`.gz`)**.
- **First-Class Database Streaming (`rowpipe db`)**: True cursor-based streaming sources and sinks for PostgreSQL, MySQL, and SQLite. "Databases are just another Rowpipe source and sink" — rows normalize directly into standard `DataBatch` objects with zero parallel data models.
- **Intelligent Pushdown Optimizer**: Automatically pushes compatible operations (`SELECT`, `WHERE`, `ORDER BY`, `LIMIT`, `OFFSET`) down into database queries in `--table` mode while seamlessly executing non-pushdownable transforms in the local streaming pipeline.
- **Database Writing, Migration & DDL**: Stream directly into database tables (`--to-db`) with automatic table creation (`--create-table`), batched inserts, transactions (`--transaction`), dialect-specific upsert (`--upsert --conflict <cols>`), and truncate (`--truncate`).
- **Lossless Type Precision**: Preserves full `BIGINT` and arbitrary precision `DECIMAL` / `NUMERIC` values without floating point corruption. Safely maps `JSONB`, `BYTEA`/`BLOB`, dates, and timestamps.
- **Connection Security & Credential Masking**: Automatically sanitizes database passwords across all logs, explain plans, errors, and metadata (`postgres://user:***@host/db`).
- **Apache Parquet Support**: High-performance columnar binary format reader (Snappy, Zstd, Gzip decompression via pure JS) and streaming writer.
- **Filesystem as Data (`rowpipe files`)**: First-class streaming filesystem source adapter. Directories and files become streams of tabular records (`path`, `size`, `type`, `extension`, `modified_at`, streaming `hash`, `mime`) composable with `filter`, `select`, `stats`, `convert`, and `diff`.
- **Cross-Format Dataset Diffing (`rowpipe diff`)**: High-performance, stream-first comparison across CSV, JSONL, XLSX, Parquet, and Database tables with bounded memory and automatic spill-to-disk. Supports composite keys, duplicate policies, value tolerance, column filtering, schema diffing, JSONL patch generation, and CI `--fail-on-diff` mode.

- **Zero-`eval` Safe Expression Engine**: Ultra-fast JIT closure-compiled evaluators supporting arithmetic (`revenue - cost`), string/date pipe functions (`email | lower | trim`), and direct JSON dot-notation (`payload.user.city`).
- **Map & Reduce Streaming Engine**: Compute row derivations (`rowpipe map`) and global or group-by aggregations (`rowpipe reduce --by country`) in a single pass.
- **Numerically Stable Streaming Stats**: Online Welford statistics (mean, variance, stddev, min, max, sum) and $O(1)$ HyperLogLog distinct cardinality estimation.
- **Incremental Schema Inference & Semantic Types**: Detects data types with confidence scoring and semantic patterns (`email`, `url`, `uuid`, `ipv4`, `country-code`, `currency`, `phone`).
- **Streaming Schema Validation**: Validates stream against JSON schemas with detailed violation reporting.
- **Reservoir Sampling**: Deterministic $O(k)$ bounded sampling with `--seed` support.
- **Unix Pipeline Friendly**: Full support for stdin/stdout, pipes, `.gz` stream decompression/compression, and clean stdout data vs. stderr progress separation.

---

## Installation

Rowpipe is available as a **standalone zero-dependency executable** (no Node.js required) as well as an npm package.

### Option 1: Standalone Single Executable (Recommended)

#### 🍏 macOS & 🐧 Linux (Universal 1-Line Installer)
```bash
curl -fsSL https://raw.githubusercontent.com/litepacks/rowpipe/main/install.sh | bash
```

#### 🍺 Homebrew (macOS & Linux)
```bash
brew tap litepacks/tap
brew install rowpipe
```
*(Or install directly from the repository formula)*
```bash
brew install litepacks/rowpipe/rowpipe
```

#### 📦 Debian / Ubuntu (`apt` / `.deb`)
```bash
curl -sLO https://github.com/litepacks/rowpipe/releases/latest/download/rowpipe_2.10.0_amd64.deb
sudo apt install -y ./rowpipe_2.10.0_amd64.deb
```

#### 🪟 Windows & GitHub Releases
Pre-compiled standalone binaries for **macOS (Apple Silicon & Intel)**, **Linux (x64)**, and **Windows (x64)** are available on the [GitHub Releases](https://github.com/litepacks/rowpipe/releases) page:
* `rowpipe-darwin-arm64.tar.gz` (macOS Apple Silicon M1/M2/M3/M4)
* `rowpipe-darwin-x64.tar.gz` (macOS Intel)
* `rowpipe-linux-x64.tar.gz` / `rowpipe_2.10.0_amd64.deb` (Linux)
* `rowpipe-win32-x64.zip` (Windows)

---

### Option 2: Via NPM (Node.js 20+)

```bash
# Global CLI installation
npm install -g rowpipe

# Or run instantly without installation
npx rowpipe --help

# Or install as a programmatic library
npm install rowpipe
```

---

## CLI Usage & Examples

### 1. Inspect Dataset & Multi-Sheet Excel / Parquet Analysis
Inspect format, row count, column list, data types, null percentage, and approximate distinct counts:

```bash
# CSV / TSV / JSONL
rowpipe inspect users.csv
rowpipe inspect data.tsv

# Apache Parquet dataset
rowpipe inspect dataset.parquet

# Machine-readable JSON output
rowpipe inspect dataset.parquet --json

# Analyze all sheets in an Excel workbook
rowpipe inspect workbook.xlsx

# Inspect a specific sheet in full detail
rowpipe inspect workbook.xlsx --sheet Users
```

### 2. Format Conversion & Export
Convert across formats without intermediate buffering:

```bash
# CSV / TSV to Apache Parquet
rowpipe convert sales.csv sales.parquet

# Parquet to Gzipped JSONL
rowpipe convert sales.parquet sales.jsonl.gz

# Filter and output directly as a GitHub Markdown Table
rowpipe filter dataset.parquet "status == 'ACTIVE' && score >= 90" --to markdown

# Export specific Excel worksheet to CSV / TSV
rowpipe convert workbook.xlsx users.tsv --sheet Users

# Bulk export ALL worksheets to individual files in a directory
rowpipe convert workbook.xlsx --all-sheets --out-dir ./exported/ --to csv

# Streaming through gzip
rowpipe convert data.csv.gz data.jsonl.gz
```

### 3. Streaming Statistics
Compute online statistics:

```bash
# Dataset-wide statistics
rowpipe stats sales.csv

# Specific column stats
rowpipe stats sales.csv --column revenue

# Excel sheet stats
rowpipe stats workbook.xlsx --sheet Orders --column total
```

Example output:
```text
Total Rows: 8,531,221

revenue (numeric)
----------------------------------------
  count        7,828,128
  null         703,093
  min          0
  max          81,231.14
  sum          651,378,531.88
  mean         83.21
  stddev       412.31
  variance     170,001.21
  distinct*    ~8,421,000

* approximate
```

### 4. Schema Inference
Infer column types, nullability, confidence percentages, and semantic patterns:

```bash
rowpipe schema users.csv

# Sample first 10,000 rows (default) or scan full dataset
rowpipe schema huge.csv --full

# Excel sheet schema inference
rowpipe schema workbook.xlsx --sheet Users
```

Example output:
```text
COLUMN       TYPE               NULLABLE   CONFIDENCE
id           integer            false      100%
name         string             false      100%
email        string (email)     true       99.4%
age          integer            true       98.1%
created_at   date               false      100%
```

### 5. Filter Rows (Safe Expression Engine & Pipe Syntax)
Filter rows using safe expressions with standard function calls or Unix-style **Pipe Syntax (`|`)** without `eval`:

```bash
# Pipe syntax for string transformations
rowpipe filter users.csv 'email | lower | trim | endsWith("@corp.com")'

# Date & time functions with pipe
rowpipe filter orders.csv 'created_at | year == 2026 && created_at | month >= 6'

# Set membership and range checks
rowpipe filter sales.csv 'country | in("TR", "US", "DE") && age | between(18, 65)'

# String split and substring
rowpipe filter users.csv 'email | splitIndex("@", 1) == "gmail.com"'
rowpipe filter users.csv 'phone | substr(0, 3) == "+90"'

# Type checking and semantic patterns
rowpipe filter users.csv 'email | isEmail && age | isNumber'

# Direct dot-notation JSON & nested object access
rowpipe filter events.csv 'payload.user.address.city == "Istanbul"'
rowpipe filter events.csv 'payload.items.0.price > 50'
rowpipe filter users.jsonl 'profile.address.country == "TR" && profile.age >= 18'

# Combining dot-notation with pipe transforms
rowpipe filter events.csv 'payload.user.email | lower | trim | endsWith("@corp.com")'
rowpipe filter events.csv 'payload.meta.score | toFloat | between(80, 100)'

# Math and numeric bounds
rowpipe filter products.csv 'price | clamp(10, 100) > 50'
rowpipe filter metrics.csv 'revenue | toFloat | round(2) >= 1000'

# Conditional evaluation (if / iif)
rowpipe filter orders.csv 'total | if(is_vip, total * 0.9, total) > 500'
```

#### Supported Operators & Functions:
- **Direct Dot-Notation**: `payload.user.city`, `payload.items.0.price`, `user.address.zip` (automatically navigates nested objects, arrays, and JSON strings)
- **Pipe Operator**: `val | func` / `val | func(arg1, arg2)` (Chained functional piping)
- **Comparison**: `==`, `!=`, `<`, `<=`, `>`, `>=`
- **Logic**: `&&`, `||`, `!`, `and`, `or`, `not`
- **Arithmetic**: `+`, `-`, `*`, `/`, `%`
- **Set & Range**: `in(val, ...)`, `notIn(val, ...)`, `between(val, min, max)`
- **Date & Time**: `year(d)`, `month(d)`, `day(d)`, `hour(d)`, `minute(d)`, `dayOfWeek(d)`, `dateDiff(d1, d2, unit)`, `isPast(d)`, `isFuture(d)`, `isToday(d)`
- **String & Text**: `contains(s, target)`, `startsWith(s, prefix)`, `endsWith(s, suffix)`, `lower(s)`, `upper(s)`, `trim(s)`, `length(s)`, `concat(...)`, `substr(s, start, len)`, `replace(s, search, rep)`, `splitIndex(s, delim, idx)`, `indexOf(s, search)`, `padLeft(s, len, char)`, `padRight(s, len, char)`, `matches(s, regex)`
- **Math & Numeric**: `abs(n)`, `round(n, dec)`, `ceil(n)`, `floor(n)`, `clamp(n, min, max)`, `min(...)`, `max(...)`, `sqrt(n)`, `pow(b, e)`, `log(n)`
- **Control Flow & Nulls**: `if(cond, then, else)`, `isNull(v)`, `isNotNull(v)`, `coalesce(...)`, `nullIf(a, b)`, `nvl(v, default)`
- **Type Inspection & Casting**: `isNumber(v)`, `isEmail(v)`, `isUrl(v)`, `isUuid(v)`, `isDate(v)`, `toInt(v)`, `toFloat(v)`, `toString(v)`, `toBool(v)`
- **JSON Navigation**: Direct dot-notation (`payload.user.city`) or fallback `jsonGet(v, "path.to.field")`


### 6. Map & Derive Columns (Row-Level Computations)
Compute and derive new columns per row in stream using expressions, arithmetic, strings, dates, or JSON dot-notation:

```bash
# Arithmetic & percentage formulas
rowpipe map sales.csv "profit=revenue - cost" "tax=revenue * 0.20" "margin=((revenue - cost) / revenue) * 100"

# String operations, dates, and dot-notation JSON mapping
rowpipe map events.csv \
  "user_city=payload.user.address.city" \
  "clean_email=email | lower | trim" \
  "year=created_at | year"

# Conditional mapping (if/else, clamp, round)
rowpipe map orders.csv "net_total=total | if(is_vip, total * 0.90, total) | round(2)"
```

### 7. Reduce & Aggregate (Global & Group-By)
Aggregate streams into summary metrics with bounded memory ($O(1)$) globally or grouped by columns (`--by`):

```bash
# 1. Global Aggregation (Single summary row)
rowpipe reduce sales.csv \
  "total_revenue=sum(revenue)" \
  "avg_margin=avg(margin)" \
  "min_price=min(price)" \
  "max_price=max(price)" \
  "total_orders=count()" \
  "unique_users=countDistinct(user_id)" \
  "rev_stddev=stddev(revenue)"

# 2. Group-By Streaming Aggregation
rowpipe reduce sales.csv \
  "total_sales=sum(revenue)" \
  "avg_profit=avg(profit)" \
  "order_count=count()" \
  --by country,category
```

Supported Reduce Functions:
- `sum(col)` — Numeric sum
- `avg(col)` / `mean(col)` — Average
- `min(col)` / `max(col)` — Minimum / Maximum
- `count()` / `count(col)` — Row or non-null count
- `countDistinct(col)` — HyperLogLog distinct cardinality ($O(1)$ memory)
- `stddev(col)` / `variance(col)` — Welford online single-pass standard deviation / variance
- `first(col)` / `last(col)` — First non-null / last value

### 8. Select Columns
Project specific columns in order:

```bash
rowpipe select users.csv id,name,email

# Select columns from Excel sheet
rowpipe select workbook.xlsx id,name,email --sheet Users
```

### 9. Rename Columns
Rename columns in stream:

```bash
rowpipe rename users.csv username=name signup_date=created_at
```

### 10. Cast Column Types
Convert types in stream with configurable error handling:

```bash
rowpipe cast users.csv age:integer revenue:number active:boolean --on-error null
```

Options for `--on-error`:
- `null` (default): Sets invalid values to `null`
- `fail`: Exits immediately with error context
- `keep`: Retains original uncast value
- `skip-row`: Omit the row from output

### 11. Reservoir Sample
Sample a fixed number of rows with bounded memory ($O(k)$) and optional deterministic seed:

```bash
rowpipe sample huge.csv --rows 1000 --seed 42
```

### 12. Schema Validation
Validate datasets against a JSON schema definition:

```bash
rowpipe validate users.csv --schema users.schema.json
```

### 13. Unix Pipes & End-to-End Map-Reduce Pipeline
Chain operations with standard Unix pipes for full stream processing:

```bash
cat transactions.csv |
  rowpipe map - "profit=revenue - cost" "margin=((revenue - cost) / revenue) * 100" |
  rowpipe filter - "margin >= 15" |
  rowpipe reduce - "total_profit=sum(profit)" "avg_margin=avg(margin)" "count=count()" --by country |
  rowpipe convert - --to jsonl > profitable_countries.jsonl
```

---

## Compare Datasets (`rowpipe diff`)

`rowpipe diff` is a production-grade, stream-first comparison engine that compares two tabular datasets by primary or composite key and reports:

- **Added rows** (present in right dataset, absent in left)
- **Removed rows** (present in left dataset, absent in right)
- **Changed rows** (matching key, different values)
- **Unchanged rows** (matching key, identical values)
- **Changed columns breakdown** (ranked by frequency of changes)
- **Schema differences** (added/removed columns and type changes)

```bash
rowpipe diff yesterday.csv today.csv --key id
```

Example Summary Output:

```text
Comparing:
  yesterday.csv
  today.csv

Key: id

Rows
  added          1,283
  removed          412
  changed        8,921
  unchanged     91,204

Changed columns
  price          5,821
  status         2,101
  email            999

Schema
  + loyalty_level: string
  ~ price: integer -> number
  - legacy_code: string
```

### Key Capabilities & Examples

#### 1. Composite Keys
Use comma-separated column names for multi-column composite keys with collision-proof typed encoding:
```bash
rowpipe diff old.csv new.csv --key country,user_id
```

#### 2. Cross-Format Comparison
Compare datasets seamlessly across any combination of supported formats (CSV, TSV, JSONL, XLSX, Apache Parquet):
```bash
# Compare CSV with Parquet
rowpipe diff old.csv new.parquet --key id

# Compare JSONL with Excel
rowpipe diff old.jsonl new.xlsx --key user_id --sheet Users

# Compare specific sheets between two Excel workbooks
rowpipe diff old.xlsx new.xlsx --left-sheet Nov --right-sheet Dec --key sku
```

#### 3. Ignore or Select Specific Columns
```bash
# Ignore volatile timestamp or audit columns
rowpipe diff old.csv new.csv --key id --ignore updated_at,last_seen

# Compare only specific columns for changes
rowpipe diff old.csv new.csv --key id --columns price,status
```

#### 4. Value Equality, Tolerance & Coercion
- **Strict by default**: `null != ""` and `0 != "0"`.
- **`--coerce`**: Coerces types (e.g. `"42" == 42`, `"true" == true`) across formats.
- **`--epsilon <val>`**: Floating point comparison tolerance (`abs(a - b) <= epsilon`).
- **`--trim` & `--ignore-case`**: String normalization options.

```bash
rowpipe diff old.csv new.csv --key id --epsilon 0.001 --trim --ignore-case
```

#### 5. Duplicate Key Policies
Duplicate keys in either dataset are never silently ignored:
- `--duplicate-key error` *(default)*: Exits immediately with row context.
- `--duplicate-key first`: Keeps the first occurrence.
- `--duplicate-key last`: Keeps the latest occurrence.

#### 6. Detailed Row & JSONL Patch Output Modes
```bash
# Detailed row change output with limit
rowpipe diff old.csv new.csv --key id --format rows --only changed --limit 50

# Stream JSONL patches (suitable for ETL change feeds or audit logs)
rowpipe diff old.csv new.csv --key id --format patch > changes.jsonl

# Machine-readable JSON summary metadata
rowpipe diff old.csv new.csv --key id --json
```

Example JSONL patch output:
```json
{"op":"update","key":{"id":24},"changes":{"price":{"old":120,"new":125}}}
{"op":"insert","key":{"id":31},"row":{"id":31,"name":"Alice"}}
{"op":"delete","key":{"id":51},"row":{"id":51,"name":"Bob"}}
```

#### 7. CI/CD Validation (`--fail-on-diff`)
Exit with non-zero code (`1`) when differences exist, or `0` when datasets are equivalent:

```bash
rowpipe diff expected.csv actual.csv \
  --key id \
  --fail-on-diff
```

This makes Rowpipe ideal for automated verification of:
- **Database migrations**
- **ETL jobs & data pipelines**
- **API export validation**
- **Regression test fixtures**
- **Scheduled report auditing**

#### 8. Bounded Memory Architecture & Automatic Spill-to-Disk
Keyed diffing is classified as **Global State / Spillable**. Rowpipe buffers indexed keys in memory and automatically spills to disk using an append-only temp file index when `--memory-limit` is reached (default: 256MB).

```bash
# Handle multi-million-row diffs with bounded RAM
rowpipe diff huge-old.csv huge-new.csv \
  --key id \
  --memory-limit 128mb
```

---

## Filesystem as Data (`rowpipe files`)

> **Everything becomes a stream of records.**

Rowpipe treats files and directories as another first-class streaming record source. A directory tree becomes a stream of metadata records that can be filtered, selected, mapped, reduced, converted, and diffed using the exact same stream pipeline used for CSV, JSONL, XLSX, and Parquet.

```bash
rowpipe files ./uploads
```

Example Output (as Table / CSV / JSONL):

```text
path                      name             ext    size       modified_at
uploads/avatar.png        avatar.png       png    182312     2026-09-16T08:13:11.923Z
uploads/users.csv         users.csv        csv    8123102    2026-09-15T19:42:01.114Z
uploads/archive/data.zip  data.zip         zip    12931231   2026-09-12T11:05:30.852Z
```

### Standard Record Fields
Each file record contains rich, standardized metadata:
- `path`: Full normalized path
- `relative_path`: Normalized path relative to root directory (uses `/` across all operating systems)
- `name`: File name with extension (e.g. `logo.png`)
- `basename`: File name without extension (e.g. `logo`)
- `extension`: Lowercase extension without dot (e.g. `png`)
- `directory`: Parent directory path
- `type`: Record type (`"file"`, `"directory"`, `"symlink"`)
- `size`: Numeric size in bytes (`0` for directories)
- `created_at`, `modified_at`, `accessed_at`: ISO 8601 timestamp strings
- `mode`: File permission mode
- `is_file`, `is_directory`, `is_symlink`: Boolean flags
- `hash` *(optional)*: Streaming content hash (`sha256`, `sha1`, `md5`, `fast`)
- `mime` & `category` *(optional)*: Inferred MIME type (`image/png`, `text/csv`) and category (`image`, `data`, `code`, `archive`, `document`, `video`, `audio`)

---

### Filesystem CLI Workflows & Examples

#### 1. Recursive Traversal & Glob Filtering
```bash
# Scan directory recursively (default)
rowpipe files ./src

# Limit traversal depth
rowpipe files . --max-depth 2

# Include specific globs
rowpipe files ./src --include "**/*.ts"

# Exclude patterns (e.g. node_modules, build artifacts)
rowpipe files . --exclude "dist/**" --exclude "*.tmp"

# Include hidden files & dot directories
rowpipe files . --hidden
```

#### 2. Streaming Hashes & MIME Type Detection
```bash
# Compute streaming SHA-256 hashes (never loads full file into memory)
rowpipe files ./assets --hash sha256

# Fast non-cryptographic fingerprinting (size + mtime + content sampling)
rowpipe files ./dataset --hash fast

# Infer MIME types and high-level categories
rowpipe files ./uploads --mime
```

#### 3. Unix Pipeline Composition
Compose filesystem metadata directly into Rowpipe streams:

```bash
# Filter large files (> 1MB)
rowpipe files ./uploads | rowpipe filter - 'size > 1000000'

# Project specific columns
rowpipe files ./src | rowpipe select - path,size,extension

# Calculate disk usage stats across file types
rowpipe files ./src --to jsonl | \
  rowpipe reduce - "total_size=sum(size)" "file_count=count()" --by extension
```

#### 4. Deterministic Snapshots & Build Verification
Create stable, reproducible file inventory snapshots for build artifacts, deployments, or backups:

```bash
# Generate deterministic snapshot
rowpipe files snapshot ./dist \
  --hash sha256 \
  --to jsonl > dist.snapshot.v1.jsonl

# Generate snapshot of next build
rowpipe files snapshot ./dist \
  --hash sha256 \
  --to jsonl > dist.snapshot.v2.jsonl

# Compare snapshots using Rowpipe's diff engine
rowpipe diff dist.snapshot.v1.jsonl dist.snapshot.v2.jsonl \
  --key relative_path \
  --ignore modified_at,accessed_at
```

#### 5. Native Directory Diffing (`rowpipe files diff`)
Compare two directory trees directly:

---

## Databases

Rowpipe provides first-class streaming database support for **PostgreSQL, MySQL, and SQLite**.

The key architectural rule is:

> **Databases are just another Rowpipe source and sink.**

Database rows normalize directly into standard Rowpipe `DataBatch` and `Row` objects. There is no parallel execution engine or secondary row type — existing operations (`filter`, `select`, `sort`, `group`, `reduce`, `stats`, `diff`) compose natively without intermediate files or memory buffering.

```text
PostgreSQL / MySQL / SQLite / CSV / JSONL / Parquet / Files
                           ↓
                     DataBatch / Row
                           ↓
             filter / select / map / sort
             group / reduce / stats / diff
                           ↓
                     TabularWriter
                           ↓
PostgreSQL / MySQL / SQLite / CSV / JSONL / Parquet / Markdown
```

### 1. Database Streaming Sources

Read directly from database tables or custom queries in bounded, cursor-based streams:

```bash
# Read from PostgreSQL table in streaming batches
rowpipe db postgres://localhost/mydb --table users

# Read from MySQL database
rowpipe db mysql://root@localhost/app --table orders

# Read from SQLite database file
rowpipe db sqlite://./data.db --table events
# or direct file path
rowpipe db ./data.db --table events

# Execute custom SQL query
rowpipe db postgres://localhost/mydb \
  --query 'SELECT id, email, created_at FROM users WHERE active = true'

# Parameterized queries (driver-level binding)
rowpipe db postgres://localhost/mydb \
  --query 'SELECT * FROM users WHERE age > $1 AND status = $2' \
  --params '[30, "ACTIVE"]'
```

### 2. Output to Any Rowpipe Format

Database streams flow directly through Rowpipe writers to CSV, JSONL, Parquet, or Markdown tables:

```bash
# Stream PostgreSQL table to JSONL
rowpipe db postgres://localhost/app --table users --to jsonl > users.jsonl

# Stream MySQL query to CSV
rowpipe db mysql://localhost/app --query 'SELECT * FROM orders' --to csv > orders.csv

# Stream SQLite table to GitHub Markdown table
rowpipe db sqlite://./sales.db --table sales --limit 20 --to markdown
```

### 3. Pipeline Operations & Pushdown Optimizer

When querying tables with `--table`, Rowpipe's **Intelligent Pushdown Optimizer** analyzes the pipeline and pushes compatible operations directly into database SQL:

* `select` $\rightarrow$ `SELECT col1, col2`
* `filter` $\rightarrow$ `WHERE condition`
* `sort` $\rightarrow$ `ORDER BY col DESC`
* `limit` $\rightarrow$ `LIMIT N`
* `offset` $\rightarrow$ `OFFSET N`

Unsupported or complex operations safely remain in the local streaming pipeline.

```bash
# Pushes down WHERE, SELECT, ORDER BY, and LIMIT to database SQL
rowpipe db postgres://localhost/app \
  --table users \
  --filter 'active == true and age > 25' \
  --select id,email,created_at \
  --sort created_at:desc \
  --limit 100 \
  --to jsonl

# Mixed execution: pushdown WHERE + local aggregations
rowpipe db sqlite://./sales.db \
  --table sales \
  --where 'year = 2026' \
  --group-by country \
  --sum revenue \
  --sort revenue_sum:desc
```

### 4. Database Sinks & File-to-Database Loading

Load CSV, JSONL, XLSX, Parquet, or other databases directly into database tables in batched transactions:

```bash
# Load CSV into PostgreSQL table with automatic DDL table creation
rowpipe users.csv \
  --to-db postgres://localhost/app \
  --to-table users \
  --create-table

# Load JSONL into SQLite database
rowpipe events.jsonl \
  --to-db sqlite://./app.db \
  --to-table events \
  --create-table

# Dialect-specific Upsert on conflict keys
rowpipe users.csv \
  --to-db postgres://localhost/app \
  --to-table users \
  --upsert \
  --conflict id

# Destructive Truncate before bulk inserting
rowpipe new_catalog.parquet \
  --to-db mysql://localhost/store \
  --to-table catalog \
  --truncate \
  --transaction
```

### 5. Database-to-Database Migrations (Zero Intermediate Files)

Stream data directly between different database dialects with on-the-fly transformations:

```bash
# Migrate PostgreSQL table directly to SQLite with filtering & projection
rowpipe db postgres://production/app \
  --table users \
  --filter 'deleted_at == null' \
  --select id,email,created_at \
  --to-db sqlite://./backup.db \
  --to-table active_users \
  --create-table
```

### 6. Cross-Format Diffing with Databases

Diff a database table directly against a CSV, JSONL, or Parquet file using Rowpipe's diff engine:

```bash
# Compare PostgreSQL table against CSV export
rowpipe diff \
  'postgres://localhost/app?table=users' \
  users_backup.csv \
  --key id \
  --coerce
```

### 7. Database Introspection

Inspect tables and schema types without third-party database clients:

```bash
# List all tables in database
rowpipe db postgres://localhost/app --tables

# Inspect column schema, data types, and nullability
rowpipe db postgres://localhost/app --schema users

# Output schema as machine-readable JSON
rowpipe db postgres://localhost/app --schema users --json
```

### 8. Lossless Type Mapping & Security

- **BIGINT**: Values outside JavaScript safe integer range (e.g. `9223372036854775807`) are preserved losslessly as `bigint` without silent precision loss.
- **DECIMAL / NUMERIC**: High-precision numbers survive database $\leftrightarrow$ file workflows without floating point truncation.
- **JSON / JSONB**: Native objects and arrays are preserved and parsed appropriately.
- **BYTEA / BLOB**: Binary buffers are mapped cleanly without dumping corrupted text to stdout.
- **Credential Masking**: Connection URLs containing passwords (e.g. `postgres://admin:secret@host/db`) are automatically masked (`postgres://admin:***@host/db`) across all logs, explain plans, progress outputs, and errors.

---

## Tabular Operations & Execution Planner

Rowpipe 2.0 introduces a comprehensive set of Unix-like tabular data operations, powered by a rule-based execution planner that optimizes multi-transform pipelines and enforces strict memory bounds.

> **Streaming is the default. Global state is explicit, bounded, and spillable.**

### Streaming Safety Classification

| Classification | Operations | Memory Complexity | Description |
|----------------|------------|-------------------|-------------|
| **Fully Streaming** | `limit`, `offset`, `head`, `filter`, `select`, `rename`, `cast`, `map`, `count` | $O(1)$ | Consumes stream in batches, cancels upstream sources immediately upon completion. |
| **Bounded State** | `tail`, `top`, `sample` | $O(K)$ / $O(N)$ | Maintains a fixed-size ring buffer ($O(N)$) or binary min/max heap ($O(K)$). |
| **Spillable Global State** | `sort`, `unique`, `group`, `reduce`, `diff` | $O(1)$ RAM threshold + Disk Spilling | Buffers in-memory runs up to `--memory-limit` and transparently spills to disk partitions or runs. |

---

### Operations Reference

#### 1. Limit & Head
Emit the first $N$ rows and immediately cancel upstream stream reading:

```bash
# Explicit limit command
rowpipe limit users.csv 100

# Pipeline flag
rowpipe users.csv --limit 100

# Head alias (default: 10 rows)
rowpipe head users.csv
rowpipe head users.csv -n 50

# Streaming through Unix pipe
cat data.csv | rowpipe limit - 100
```

#### 2. Offset
Skip the first $N$ rows and stream everything after without scanning unneeded rows:

```bash
rowpipe offset users.csv 1000
rowpipe users.csv --offset 1000

# Combine with limit (automatically halts reading after 1,100 rows)
rowpipe users.csv --offset 1000 --limit 100
```

#### 3. Tail
Emit the last $N$ rows using a bounded circular ring buffer without buffering the entire dataset in RAM:

```bash
# Default: last 10 rows
rowpipe tail server_logs.jsonl

# Specify row count
rowpipe tail server_logs.jsonl -n 100
```

#### 4. External Merge Sort (`sort`)
Multi-column typed sorting with deterministic sequence stability, natural sorting, case-insensitivity, null handling, and disk run spilling:

```bash
# Single column sort (numeric, boolean, date, string typed)
rowpipe sort users.csv --by age:desc

# Multi-column sort
rowpipe sort sales.csv --by country --by revenue:desc

# Compact syntax
rowpipe sort sales.csv --by "country,revenue:desc"

# Natural filename / alphanumeric sort
rowpipe sort files.csv --by name --natural

# Null value positioning
rowpipe sort users.csv --by age --nulls first   # or --nulls last

# Memory threshold before disk run spilling
rowpipe sort huge.csv --by created_at --memory-limit 256mb --temp-dir /tmp/rowpipe
```

#### 5. Top-K Bounded Heap (`top`)
Extract the top (or bottom) $K$ records using a bounded $O(K)$ binary heap:

```bash
# Top 10 largest by revenue
rowpipe top sales.csv --by revenue -n 10

# Top 20 largest files
rowpipe files ./src --sort size:desc --limit 20   # Automatically rewritten to TopK(20)!

# Bottom 5 smallest
rowpipe top users.csv --by score -n 5 --smallest
```

#### 6. Unique Deduplication (`unique`)
Deduplicate rows by key columns or entire rows with spillable hash partitions:

```bash
# Unique by single field (keeps first occurrence by default)
rowpipe unique users.csv --by email

# Composite uniqueness
rowpipe unique users.csv --by country,email

# Keep last occurrence
rowpipe unique logs.jsonl --by user_id --keep last

# Entire row deduplication
rowpipe unique dataset.csv
```

#### 7. Count Analyzer (`count`)
Ultra-lightweight streaming row, distinct, and group counting:

```bash
# Total row count
rowpipe count dataset.parquet
# Output: 8,531,221

# Distinct column count (exact or approximate HyperLogLog)
rowpipe count users.csv --distinct email
rowpipe count users.csv --distinct email --approx

# Group-by count
rowpipe count users.csv --by country

# Machine-readable JSON output
rowpipe count users.csv --by country --json
```

#### 8. Group & Aggregations (`group`)
Group tabular streams with single-pass hash accumulators and multi-column aggregations:

```bash
# Group count
rowpipe group sales.csv --by country --count

# Multi-field aggregations
rowpipe group sales.csv \
  --by country \
  --count \
  --sum revenue \
  --avg revenue \
  --max quantity

# Compact aggregation spec
rowpipe group sales.csv \
  --by country,category \
  --agg "orders=count(),total_rev=sum(revenue),avg_rev=avg(revenue)" \
  --to markdown
```

---

### Combined Multi-Transform Pipeline

Chain multiple operations in a **single process** without writing temporary intermediate files:

```bash
# Reader -> Filter -> Select -> TopK -> Writer
rowpipe users.csv \
  --filter "age >= 18 && active == true" \
  --select id,name,age,country,revenue \
  --sort revenue:desc \
  --limit 50 \
  --to markdown
```

### Execution Plan Visualizer (`rowpipe explain`)

Inspect the planned execution pipeline and see automatic optimizations in action:

```bash
rowpipe explain sales.csv \
  --filter "active == true" \
  --sort revenue:desc \
  --limit 10 \
  --to jsonl
```

Output:
```text
======================================================
              Rowpipe Execution Plan                  
======================================================

  CSVReader (sales.csv)
    ↓
  Filter (active == true)
    ↓
  TopK (revenue DESC, 10) [Bounded Heap]
    ↓
  JSONLWriter (stdout)

------------------------------------------------------
Optimizations Applied:
  • sort(revenue desc) + limit(10) -> top-k (10)
------------------------------------------------------
Memory Classification:
  Bounded State (O(K) fixed memory bound)
======================================================
```

---

## Node.js Library API

Rowpipe can be imported directly in your Node.js projects:

```typescript
import {
  createPipeline,
  CSVReader,
  JSONLWriter,
  XLSXReader,
  ParquetReader,
  FileSystemReader,
  filterRows,
  selectColumns,
  limitRows,
  offsetRows,
  tailRows,
  sortRows,
  topRows,
  uniqueRows,
  groupRows,
  countStream,
  diffRows,
  computeDiff,
} from "rowpipe";
import { createReadStream, createWriteStream } from "node:fs";

// Fluent Pipeline API
const reader = new CSVReader(createReadStream("sales.csv"));
const writer = new JSONLWriter(createWriteStream("top_sales.jsonl"));

await createPipeline(reader)
  .filter("status == 'completed'")
  .sort({ by: "revenue:desc" })
  .limit(100)
  .to(writer);

// Filesystem as Tabular Source + Top 10 Largest Files
const fsReader = new FileSystemReader({ root: "./src" });
const largestFiles = await createPipeline(fsReader)
  .top({ by: "size:desc", count: 10 })
  .toArray();
```

---

## Exit Codes

Rowpipe uses standardized POSIX exit codes:

| Code | Meaning | Description |
|------|---------|-------------|
| `0`  | Success | Command executed successfully |
| `1`  | Generic / Diff Mismatch | Runtime error or diff mismatch when `--fail-on-diff` is specified |
| `2`  | Invalid Arguments | Missing or invalid CLI flags or arguments |
| `3`  | Parse Error | Corrupted or malformed CSV/JSON/XLSX input with row/column context |
| `4`  | Validation Failure | Schema validation violations detected |
| `5`  | Duplicate Key Error | Duplicate key encountered during keyed diff under `--duplicate-key error` |

---

## Memory & Performance Benchmarks

### 1. Operations Benchmark (`1,000,000 rows`, 47.1 MB CSV):

| Scenario | Throughput | Elapsed Time | Peak RSS Memory | Memory Scaling |
|----------|------------|--------------|-----------------|----------------|
| **Limit 100** (Early Stream Cancel) | **Instant** | 0.01s | ~492 MB | $O(1)$ Early Termination |
| **Tail 100** (Bounded RingBuffer) | **1,029,866 rows/s** (48.5 MB/s) | 0.97s | ~514 MB | $O(N)$ Bounded |
| **Top-100** (Bounded Min-Heap) | **731,529 rows/s** (34.4 MB/s) | 1.37s | ~530 MB | $O(K)$ Bounded |
| **Count + Distinct** (Streaming HLL) | **764,526 rows/s** (36.0 MB/s) | 1.31s | ~530 MB | $O(1)$ Bounded |
| **Group-by** (Sum/Avg/Min/Max) | **611,995 rows/s** (28.8 MB/s) | 1.63s | ~516 MB | $O(1)$ Stream Hash |
| **Unique** (Composite Deduplication) | **532,481 rows/s** (25.1 MB/s) | 1.88s | ~516 MB | Spillable Hash |
| **External Merge Sort** (16MB threshold) | **73,185 rows/s** (3.4 MB/s) | 13.66s | ~606 MB | Spillable $K$-Way Merge |

### 2. Filesystem Streaming Benchmark (`rowpipe files`):

| File Count | Operation | Throughput | Peak RSS Memory | Memory Scaling |
|------------|-----------|------------|-----------------|----------------|
| **50,000 files** | Stream Traversal + Metadata | **~24,100 files/s** | ~147 MB | $O(1)$ Bounded |
| **50,000 files** | Filter + Select Pipeline | **~13,800 files/s** | ~147 MB | $O(1)$ Bounded |
| **50,000 files** | Fast Fingerprint Hashing | **~3,470 files/s** | ~148 MB | $O(1)$ Bounded |

To run the benchmarks locally:

```bash
npm run bench
npm run benchmark:ops
npm run benchmark:diff
npm run benchmark:files
```

To enable real-time memory debugging on any command:

```bash
ROWPIPE_DEBUG_MEMORY=1 rowpipe files ./huge-directory --hash fast
```

---

## License

MIT


