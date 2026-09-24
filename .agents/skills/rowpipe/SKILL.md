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
| **Corr** | `rowpipe corr <file> [--cols <c1,c2,...>]` | Online single-pass Pearson correlation matrix across numeric columns |
| **Quantiles** | `rowpipe quantiles <file> <col> [--p 50,90,95,99]` | Streaming percentiles (P50, P90, P95, P99, P99.9, Median, IQR) |
| **Outliers** | `rowpipe outliers <file> <col> [--method zscore\|iqr\|mad]` | Anomaly & outlier detection using Z-Score, IQR, and Median Absolute Deviation |
| **Crosstab** | `rowpipe crosstab <file> <rowCol> <colCol> [--normalize row]` | 2D contingency table and Chi-Square ($\chi^2$) independence test |
| **Regression** | `rowpipe regression <file> <xCol> <yCol>` | Streaming Ordinary Least Squares linear regression ($y = mx + b$, $R^2$, Pearson $r$) |
| **RFM** | `rowpipe rfm <file> [--summary]` | Recency, Frequency, Monetary customer segmentation & lifecycle scoring |
| **Cohort** | `rowpipe cohort <file> [--interval 1mo] [--counts]` | User retention cohort matrix with time offset retention rates |
| **Funnel** | `rowpipe funnel <file> --steps "view->cart->buy"` | Multi-step conversion funnel, drop-off rates, and visual ASCII funnel |
| **A/B Test** | `rowpipe abtest <file> --group <col> --metric <col>` | Statistical significance testing (Two-Proportion Z-Test & Welch's T-Test) |
| **Pareto** | `rowpipe pareto <file> --item <col> --value <col>` | 80/20 rule distribution curve & ABC inventory/revenue classification |
| **Timeseries** | `rowpipe timeseries <file> --time <col> [--every 1d] [--window 7] [--lag 1] [--diff <col>]` | Time series resampling, window rollups, lag/lead, delta, and pct_change |
| **Forecast** | `rowpipe forecast <file> --time <col> --metric <col> [--steps 7] [--model holt-winters\|ar\|linear]` | Time series forecasting (Holt-Winters double/triple exponential smoothing, AR, Linear) |
| **EWMA** | `rowpipe ewma <file> --value <col> [--alpha 0.2] [--span 10] [--halflife 5]` | Exponential weighted moving averages and streaming trend smoothing |
| **Decay** | `rowpipe decay <file> --time <col> --value <col> --halflife 7d` | Time-based exponential decay scoring for recency and engagement |
| **Normality** | `rowpipe normality <file> <col> [--test shapiro\|jarque-bera\|dagostino]` | Statistical normality testing (Shapiro-Wilk, Jarque-Bera, D'Agostino-Pearson) |
| **ANOVA** | `rowpipe anova <file> --group <col> --value <col>` | One-Way Analysis of Variance across groups (F-statistic, p-value, eta-squared effect size) |
| **Mann-Whitney**| `rowpipe mannwhitney <file> --group <col> --value <col>` | Non-parametric rank-sum hypothesis test (Mann-Whitney U, z-score, p-value) |
| **Kurtosis** | `rowpipe kurtosis <file> <col> [--excess]` | Higher-order Fisher-Pearson skewness and excess kurtosis distribution shape analysis |
| **Technical** | `rowpipe technical <file> --indicators "sma(14),rsi(14),macd(12,26,9),bollinger(20,2)"` | Streaming financial technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, VWAP, ATR) |
| **Cluster** | `rowpipe cluster <file> --cols <c1,c2,...> -k 3 [--summary]` | Bounded O(1) streaming Mini-Batch K-Means clustering and row cluster assignment |
| **Entropy** | `rowpipe entropy <file> [--target <col>]` | Shannon entropy, mutual information, and feature importance rankings |
| **Ngrams** | `rowpipe ngrams <file> <textCol> [--n 2] [--top 20] [--remove-stopwords]` | Streaming text tokenization, N-gram frequency distribution, and ASCII bar charts |
| **Pivot** | `rowpipe pivot <file> --index <cols> --columns <col> --values <col> [--agg sum]` | Multi-dimensional pivot table with aggregated metrics across dimensions |
| **Unpivot** | `rowpipe unpivot <file> --index <cols> [--columns <cols>] [--var-col var] [--val-col val]` | Wide-to-long dataset melt/reshape with bounded O(1) streaming memory |
| **Fuzzy Join** | `rowpipe fuzzy-join <left> <right> --on <col> [--method levenshtein] [--threshold 0.75]` | Approximate string matching join (Levenshtein, Jaro-Winkler, Jaccard, Soundex) |
| **Concat** | `rowpipe concat <files...> [--source-col <name>]` | Stream concatenate multiple files sequentially with schema union |
| **Generate** | `rowpipe generate "<schema>" --rows <n>` | Streaming synthetic test data generation (`seq`, `uuid`, `name`, `email`, `int`, `float`, `date`, `choice`) |
| **Mask** | `rowpipe mask <file> --email <col> --phone <col> --hash <col>` | Streaming PII redaction and deterministic salted HMAC/SHA-256 tokenization |
| **Test** | `rowpipe test <file> --assert "<expr>" --not-null <cols> --unique <cols>` | Streaming data quality assertion test runner with exit codes for CI/CD |
| **Serve** | `rowpipe serve <file> [--port 3000] [--host 0.0.0.0]` | Zero-dependency streaming HTTP REST API server with query filters |
| **Report** | `rowpipe report <file> [--output report.html] [--open]` | Offline self-contained interactive HTML data health dashboard |
| **Fetch** | `rowpipe fetch <url> [--paginate page\|offset\|cursor] [--data-path items]` | Stream remote JSON/CSV REST APIs with pagination and envelope extraction |

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

### 6. Statistical Analytics & Anomaly Detection
```bash
# Multi-column Pearson correlation matrix rendered as clean Unicode table
rowpipe corr spotify.csv --cols danceability,energy,loudness,tempo,valence --to table

# Calculate key quantiles & percentiles (P50, P90, P95, P99, IQR)
rowpipe quantiles dataset.csv response_time_ms --p 50,90,95,99,99.9

# Outlier & anomaly detection (filter only anomalous records via Z-Score, IQR, or MAD)
rowpipe outliers transactions.csv amount --method zscore --threshold 3.0 --only-outliers
rowpipe outliers server_logs.csv latency --method iqr --add-columns --to table

# 2-way categorical crosstab contingency table with Chi-Square test
rowpipe crosstab titanic.csv Sex Survived --to table
rowpipe crosstab survey.csv department satisfaction --normalize row --to table

# Linear regression & trend fitting (y = mx + b, R², Pearson r, formula)
rowpipe regression sales.csv advertising_budget total_revenue --to table
```

### 7. Business & Growth BI Analytics
```bash
# E-Commerce RFM Customer Segmentation & Lifecycle Rollup
rowpipe rfm orders.csv --customer-id uid --amount price --date-col created_at --summary --to table
rowpipe rfm orders.csv --as-of 2024-01-01 --to csv > segmented_customers.csv

# User Retention Cohort Matrix over Months/Weeks/Days
rowpipe cohort user_events.csv --user-id uid --time-col timestamp --interval 1mo --to table
rowpipe cohort active_logs.parquet --interval 1w --counts --to table

# Conversion Funnel & Drop-off Analysis with Visual Funnel Bars
rowpipe funnel app_events.csv --steps "view,cart,checkout,purchase" --user-id uid --step-col event --to table

# A/B Test Statistical Significance (Two-Proportion Z-Test & Welch's T-Test)
rowpipe abtest experiment.csv --group variant --metric converted --control A --variant B --to table
rowpipe abtest pricing_test.csv --group test_group --metric revenue --type means --to table

# 80/20 Pareto & ABC Inventory / Revenue Classification
rowpipe pareto products.csv --item sku --value revenue --summary --to table
rowpipe pareto customers.csv --item customer_name --value total_spend --to table
```

### 8. Time Series, Forecasting & Decay
```bash
# 1. Resample and window rolling aggregations over time
rowpipe timeseries metrics.csv --time timestamp --every 1d --agg "sum(reqs),avg(latency)" --to table
rowpipe timeseries stock.csv --time date --window 7 --agg "avg(close)" --lag 1 --diff close --pct-change close

# 2. Forecasting future horizons (Holt-Winters Double/Triple Exponential Smoothing, AR, Linear)
rowpipe forecast revenue.csv --time date --metric revenue --steps 7 --model holt-winters --to table
rowpipe forecast signups.csv --time date --metric count --steps 14 --model ar --order 3 --to table

# 3. Streaming Exponential Weighted Moving Average (EWMA)
rowpipe ewma sensor.csv --value temp --alpha 0.2 --add-column --to table
rowpipe ewma stock.csv --value price --span 20 --to csv > smoothed.csv

# 4. Time-Based Half-Life Exponential Decay Scoring
rowpipe decay user_activity.csv --time last_active --value points --halflife 7d --add-column --to table
```

### 9. Advanced Statistical Testing & Distribution Shape
```bash
# 1. Normality testing (Shapiro-Wilk, Jarque-Bera, D'Agostino-Pearson)
rowpipe normality latencies.csv response_time --test all --to table

# 2. One-Way ANOVA across multiple categorical groups
rowpipe anova treatment_data.csv --group variant --value conversion_rate --to table

# 3. Non-parametric Mann-Whitney U Rank-Sum test (Wilcoxon rank-sum)
rowpipe mannwhitney experiment.csv --group test_group --value latency_ms --to table

# 4. Higher-order Fisher-Pearson Skewness & Excess Kurtosis
rowpipe kurtosis returns.csv daily_return --excess --to table
```

### 10. Financial Technical Analysis, ML Clustering & NLP
```bash
# 1. Financial Technical Indicators (Streaming SMA, EMA, RSI, MACD, Bollinger Bands, VWAP, ATR)
rowpipe technical ohlcv.csv --price close --high high --low low --vol volume \
  --indicators "sma(14),ema(20),rsi(14),macd(12,26,9),bollinger(20,2),vwap,atr(14)" --to table

# 2. O(1) Streaming Mini-Batch K-Means Clustering & Row Tagging
rowpipe cluster customers.csv --cols age,income,score -k 3 --summary --to table
rowpipe cluster points.csv --cols x,y,z -k 5 --max-iterations 20 --to jsonl > clustered.jsonl

# 3. Information Entropy, Mutual Information & Feature Importance Ranking
rowpipe entropy dataset.csv --target churn --to table

# 4. Text Tokenization, N-grams & Frequency Bar Charts
rowpipe ngrams reviews.csv comment --n 2 --top 20 --remove-stopwords --chart
rowpipe ngrams feedback.csv message --n 1 --min-freq 5 --to table
```

### 11. Data Engineering, Reshaping & Fuzzy Matching
```bash
# 1. Multi-Dimensional Pivot Table (Sum, Avg, Min, Max, Count, Custom Fill)
rowpipe pivot sales.csv --index region --columns year --values amount --agg sum --to table
rowpipe pivot employees.csv --index dept,role --columns year --values salary --agg avg --fill "N/A"

# 2. Wide-to-Long Dataset Reshaping (Melt / Unpivot with O(1) Memory)
rowpipe unpivot quarterly_reports.csv --index company_id --var-col quarter --val-col revenue --to table
rowpipe unpivot survey_wide.csv --index user_id --columns q1,q2,q3,q4 --drop-null --to csv > melted.csv

# 3. Fuzzy Approximate String Join (Levenshtein, Jaro-Winkler, Jaccard, Soundex)
rowpipe fuzzy-join vendors.csv master_catalog.csv --left-key name --right-key vendor_name --threshold 0.75 --score-col match_score --to table
rowpipe fuzzy-join leads.csv crm.csv --on company --method jaro-winkler --threshold 0.85 --all-matches

# 4. Sequential Multi-File Streaming Concat with Origin Filename Tagging
rowpipe concat jan.csv feb.csv mar.csv --source-col source_file --to table
rowpipe concat logs_*.jsonl --source-col origin --to parquet > merged.parquet
```

### 12. DataOps, Security & Developer Experience (DX)
```bash
# 1. High-Throughput Synthetic Test Data Generator (> 1,000,000 rows/s)
rowpipe generate "id:seq,name:name,email:email,age:int(18,65),plan:choice(free,pro,enterprise)" --rows 10000 --to csv > mock_users.csv
rowpipe mock "id:uuid,score:float(0.0,100.0,2),signup:date(2025-01-01,2026-12-31)" -n 50000 --to parquet > dataset.parquet

# 2. Streaming PII Redaction & Salted Cryptographic Tokenization
rowpipe mask customers.csv --email email --phone mobile --card cc_number --ip client_ip --name full_name --to table
rowpipe mask sensitive.csv --hash user_id --salt "custom_crypto_salt_2026" --to csv > masked.csv

# 3. Data Quality Assertion Test Runner (CI/CD Quality Gates with Exit Codes)
rowpipe test orders.csv --assert "amount > 0" --assert "discount <= amount" --not-null customer_id,created_at --unique order_id --fail-fast
rowpipe assert metrics.parquet --min-rows 1000 --max-rows 5000000 --json

# 4. Instant Zero-Dependency HTTP REST API Endpoint (Query filters, format switching)
rowpipe serve sales.parquet --port 8080 --host 0.0.0.0
# Query: curl "http://localhost:8080/rows?limit=10&filter=amount>100&format=json"
# Schema: curl "http://localhost:8080/schema"
# Stats:  curl "http://localhost:8080/stats"

# 5. Offline Interactive HTML Data Health Dashboard & Profile Report
rowpipe report dataset.parquet --output health_report.html --open

# 6. Stream Tabular Data from Remote REST APIs with Pagination
rowpipe fetch "https://api.github.com/repos/facebook/react/issues" --paginate page --page-size 50 --max-pages 10 --to table
rowpipe fetch "https://api.example.com/v1/orders" --bearer "secret_token" --data-path "response.items" --to parquet > orders.parquet
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

---

## 13. 🛡️ Fault-Tolerant Streaming & Dead-Letter Queue

Handle corrupted datasets, unclosed quotes, malformed JSON lines, and transformation errors without breaking the stream.

### CLI Options
- `--on-error <abort|skip|log>`:
  - `abort` (default): Immediately stops execution on the first corrupt row with line and column context.
  - `skip`: Silently bypasses corrupted rows and continues streaming valid records.
  - `log`: Continues streaming while logging failed rows and error details.
- `--bad-rows-log <filepath>`: Writes rejected rows, raw content, and error reasons to a structured JSONL dead-letter queue.

### Examples
```bash
# 1. Skip corrupted lines in a messy 10GB CSV file
rowpipe convert dirty.csv clean.parquet --on-error skip

# 2. Log malformed rows to dead-letter queue for auditing
rowpipe convert messy.jsonl out.csv --on-error log --bad-rows-log dead-letter.jsonl

# 3. Fault-tolerant mapping with bad row logging
rowpipe map sales.csv "profit = revenue - cost" --on-error log --bad-rows-log map_errors.jsonl
```



