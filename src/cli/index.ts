#!/usr/bin/env node

import { existsSync } from "node:fs";
import { Command } from "commander";
import { RowpipeError } from "../core/errors.js";
import { castCommand } from "./commands/cast.js";
import { cleanCommand } from "./commands/clean.js";
import { convertCommand } from "./commands/convert.js";
import { countCommand } from "./commands/count.js";
import { dbCommand, dbSchemaCommand, dbTablesCommand } from "./commands/db.js";
import { diffCommand } from "./commands/diff.js";
import { explainCommand } from "./commands/explain.js";
import { filesCommand, filesDiffCommand, filesSnapshotCommand } from "./commands/files.js";
import { filterCommand } from "./commands/filter.js";
import { groupCommand } from "./commands/group.js";
import { headCommand } from "./commands/head.js";
import { inspectCommand } from "./commands/inspect.js";
import { joinCommand } from "./commands/join.js";
import { limitCommand } from "./commands/limit.js";
import { mapCommand } from "./commands/map.js";
import { offsetCommand } from "./commands/offset.js";
import { unifiedPipelineCommand } from "./commands/pipeline.js";
import { profileCommand } from "./commands/profile.js";
import { reduceCommand } from "./commands/reduce.js";
import { renameCommand } from "./commands/rename.js";
import { sampleCommand } from "./commands/sample.js";
import { schemaCommand } from "./commands/schema.js";
import { selectCommand } from "./commands/select.js";
import { sortCommand } from "./commands/sort.js";
import { statsCommand } from "./commands/stats.js";
import { tailCommand } from "./commands/tail.js";
import { topCommand } from "./commands/top.js";
import { uniqueCommand } from "./commands/unique.js";
import { validateCommand } from "./commands/validate.js";
import { windowCommand } from "./commands/window.js";
import { viewCommand } from "./commands/view.js";
import { explodeCommand } from "./commands/explode.js";
import { flattenCommand } from "./commands/flatten.js";
import { freqCommand } from "./commands/freq.js";
import { plotCommand } from "./commands/plot.js";
import { tableCommand } from "./commands/table.js";
import { completionCommand } from "./commands/completion.js";
import { partitionCommand } from "./commands/partition.js";
import { splitCommand } from "./commands/split.js";
import { timeseriesCommand } from "./commands/timeseries.js";
import { corrCommand } from "./commands/corr.js";
import { quantilesCommand } from "./commands/quantiles.js";
import { outliersCommand } from "./commands/outliers.js";
import { crosstabCommand } from "./commands/crosstab.js";
import { regressionCommand } from "./commands/regression.js";
import { rfmCommand } from "./commands/rfm.js";
import { cohortCommand } from "./commands/cohort.js";
import { funnelCommand } from "./commands/funnel.js";
import { abTestCommand } from "./commands/abtest.js";
import { paretoCommand } from "./commands/pareto.js";
import { technicalCommand } from "./commands/technical.js";
import { clusterCommand } from "./commands/cluster.js";
import { entropyCommand } from "./commands/entropy.js";
import { ngramsCommand } from "./commands/ngrams.js";
import { pivotCommand } from "./commands/pivot.js";
import { unpivotCommand } from "./commands/unpivot.js";
import { fuzzyJoinCommand } from "./commands/fuzzy-join.js";
import { concatCommand } from "./commands/concat.js";
import { generateCommand } from "./commands/generate.js";
import { maskCommand } from "./commands/mask.js";
import { testCommand } from "./commands/test.js";
import { serveCommand } from "./commands/serve.js";
import { reportCommand } from "./commands/report.js";
import { fetchCommand } from "./commands/fetch.js";

// Handle broken pipe gracefully when piping to head/less
process.stdout.on("error", (err: unknown) => {
  if ((err as NodeJS.ErrnoException).code === "EPIPE") {
    process.exit(0);
  }
});

const program = new Command();

program
  .name("rowpipe")
  .description("Stream-first tabular data toolkit for CSV, TSV, JSON, JSONL, XLSX, Parquet, and Markdown")
  .version("2.10.0");

// Global & Pipeline options on root command
program
  .argument("[input]", "Input dataset file path or '-' for stdin")
  .option("--filter <expression>", "Filter rows using expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Project subset of columns (comma-separated)")
  .option("--rename <specs...>", "Rename columns (old=new)")
  .option("--cast <specs...>", "Cast column types (col:type)")
  .option("--map <specs...>", "Derive new columns (col=expr)")
  .option("--window <specs...>", "Compute sliding window/rolling values (col=fn(...))", (val, prev: string[] = []) => [...prev, val])
  .option("--partition-by <columns>", "Partition window functions by columns (comma-separated)")
  .option("--explode <column>", "Expand delimited or array column into multiple rows", (val, prev: string[] = []) => [...prev, val])
  .option("--explode-delimiter <delim>", "Delimiter for string exploding (default: ',')")
  .option("--flatten [sep]", "Flatten nested JSON objects into flat dot-notated columns")
  .option("--add-filename", "Inject source filename into records as '_file' column")
  .option("--file-col <name>", "Custom column name for injected filename")
  .option("--sort <specs>", "Sort stream by columns (--sort country,revenue:desc)")
  .option("--top <number>", "Retain top-N rows using bounded heap")
  .option("--limit <number>", "Limit number of rows emitted")
  .option("--offset <number>", "Skip first N rows")
  .option("--tail <number>", "Emit last N rows using bounded ring buffer")
  .option("--unique [columns]", "Deduplicate rows by key columns or entire row")
  .option("--group-by <columns>", "Group by columns (comma-separated)")
  .option("--count", "Aggregate count of rows per group")
  .option("--sum <columns>", "Sum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--avg <columns>", "Average column values", (val, prev: string[] = []) => [...prev, val])
  .option("--min <columns>", "Minimum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--max <columns>", "Maximum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--first <columns>", "First column values", (val, prev: string[] = []) => [...prev, val])
  .option("--last <columns>", "Last column values", (val, prev: string[] = []) => [...prev, val])
  .option("--agg <specs>", "Aggregation specifications", (val, prev: string[] = []) => [...prev, val])
  .option("--nulls <placement>", "Null sorting placement: first or last", "last")
  .option("--ignore-case", "Case-insensitive string sorting/comparison")
  .option("--natural", "Natural numeric sorting for alphanumeric strings")
  .option("--memory-limit <size>", "Memory limit before spilling to disk (e.g. 256mb, 64mb)", "256mb")
  .option("--temp-dir <dir>", "Custom temporary directory for disk spills")
  .option("--from <format>", "Input format (csv, tsv, psv, json, jsonl, xlsx, parquet)")
  .option("--to <format>", "Output format (csv, tsv, psv, json, jsonl, xlsx, parquet, markdown)")
  .option("--sheet <sheet>", "Worksheet name or index for XLSX")
  .option("--delimiter <delim>", "Custom CSV/TSV delimiter")
  .option("--json", "Output machine-readable JSON")
  .option("--output <file>", "Write output to file instead of stdout")
  .option("--batch-size <number>", "Processing batch size in rows", "1000")
  .option("--gzip", "Use GZIP compression/decompression")
  .option("--brotli", "Use Brotli compression/decompression")
  .option("--zstd", "Use Zstandard compression/decompression")
  .option("-z, --compress <algo>", "Compression algorithm (gzip, brotli, zstd, deflate, none)")
  .option("--to-db <url>", "Write output stream to target database (Postgres, MySQL, SQLite)")
  .option("--to-table <table>", "Target database table name")
  .option("--table <table>", "Database table name (source or target)")
  .option("--create-table", "Automatically create target database table from inferred schema")
  .option("--upsert", "Perform upsert (insert or update on conflict) into database")
  .option("--conflict <columns>", "Conflict key columns for database upsert (comma-separated)")
  .option("--transaction", "Wrap database writes in transactions", true)
  .option("--no-transaction", "Disable transactions for database writes")
  .option("--truncate", "Truncate target table before writing (destructive)")
  .option("--dry-run", "Print generated DDL/SQL without executing")
  .option("--quiet", "Suppress non-data output and progress")
  .option("--no-progress", "Disable real-time progress bar")
  .option("--on-error <strategy>", "Error handling strategy: abort (default), skip, or log", "abort")
  .option("--bad-rows-log <file>", "Write malformed or rejected rows to dead-letter log file")
  .action(async (input = "-", cmdOptions) => {
    // If running root command with no input file and stdin is an interactive TTY, display help
    if (input === "-" && process.stdin.isTTY) {
      program.outputHelp();
      return;
    }
    await unifiedPipelineCommand(input, cmdOptions);
  });

// 1. inspect
program
  .command("inspect [input]")
  .description("Inspect format, row count, columns, and sheet summary")
  .option("--sheet <sheet>", "Target worksheet name or index for XLSX")
  .option("--path <path>", "Nested object path for JSON (e.g. data.results)")
  .option("--delimiter <delim>", "Custom CSV/TSV delimiter")
  .option("--json", "Output metadata as machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await inspectCommand(input, opts);
  });

// 2. convert
program
  .command("convert [input] [output]")
  .description("Stream convert tabular datasets across formats")
  .option("--from <format>", "Input format (csv, tsv, psv, json, jsonl, xlsx, parquet)")
  .option("--to <format>", "Output format (csv, tsv, psv, json, jsonl, xlsx, parquet, markdown)")
  .option("--gzip", "Use GZIP compression")
  .option("--brotli", "Use Brotli compression")
  .option("--zstd", "Use Zstandard compression")
  .option("-z, --compress <algo>", "Compression algorithm (gzip, brotli, zstd, deflate, none)")
  .option("--sheet <sheet>", "Worksheet name or index for XLSX")
  .option("--all-sheets", "Export all worksheets in the workbook to individual files")
  .option("--out-dir <dir>", "Output directory for --all-sheets export")
  .option("--path <path>", "Nested path for JSON array")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .option("--no-header", "Disable writing or reading headers in CSV")
  .option("--on-error <strategy>", "Error handling strategy: abort (default), skip, or log")
  .option("--bad-rows-log <file>", "Write malformed or rejected rows to dead-letter log file")
  .action(async (input = "-", output, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await convertCommand(input, output, opts);
  });

// 3. schema
program
  .command("schema [input]")
  .description("Infer column types, nullability, and semantic annotations")
  .option("--sample <rows>", "Number of rows to sample for inference", "10000")
  .option("--full", "Scan full stream for exact schema inference")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name or index for XLSX")
  .option("--path <path>", "Nested path for JSON")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .option("--json", "Output schema as machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await schemaCommand(input, opts);
  });

// 4. stats
program
  .command("stats [input]")
  .description("Compute streaming statistics (Welford numeric stats and HLL distinct counts)")
  .option("--column <name>", "Compute statistics only for specific column")
  .option("--fast", "Use approximate algorithms for distinct counts")
  .option("--exact", "Use exact counts where available")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .option("--json", "Output stats as machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await statsCommand(input, opts);
  });

// 5. select
program
  .command("select [input] [columns]")
  .description("Select a subset of columns in stream")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrCols, maybeCols, cmdOptions) => {
    let input = "-";
    let columns = "";

    if (maybeCols !== undefined) {
      input = inputOrCols || "-";
      columns = maybeCols;
    } else {
      columns = inputOrCols || "";
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await selectCommand(input, columns, opts);
  });

// 6. rename
program
  .command("rename <args...>")
  .description("Rename columns in stream (e.g. rowpipe rename users.csv old=new)")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (args, cmdOptions) => {
    let input = "-";
    let specs: string[] = [];

    if (args[0] && !args[0].includes("=")) {
      input = args[0];
      specs = args.slice(1);
    } else {
      specs = args;
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await renameCommand(input, specs, opts);
  });

// 7. cast
program
  .command("cast <args...>")
  .description("Cast column types in stream (e.g. rowpipe cast users.csv age:number active:boolean)")
  .option("--on-error <behavior>", "Error behavior: fail, null, keep, skip-row", "null")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (args, cmdOptions) => {
    let input = "-";
    let specs: string[] = [];

    if (args[0] && !args[0].includes(":")) {
      input = args[0];
      specs = args.slice(1);
    } else {
      specs = args;
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await castCommand(input, specs, opts);
  });

// 8. filter
program
  .command("filter [input] [expression]")
  .description("Filter rows using safe expression engine (e.g. rowpipe filter users.csv 'age > 30')")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrExpr, maybeExpr, cmdOptions) => {
    let input = "-";
    let expression = "";

    if (maybeExpr !== undefined) {
      input = inputOrExpr || "-";
      expression = maybeExpr;
    } else {
      expression = inputOrExpr || "";
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await filterCommand(input, expression, opts);
  });

// 9. map
program
  .command("map <args...>")
  .description("Transform and derive columns per row (e.g. rowpipe map sales.csv 'profit=revenue-cost')")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (args, cmdOptions) => {
    let input = "-";
    let specs: string[] = [];

    if (args[0] && !args[0].includes("=")) {
      input = args[0];
      specs = args.slice(1);
    } else {
      specs = args;
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await mapCommand(input, specs, opts);
  });

// 10. reduce
program
  .command("reduce <args...>")
  .description("Aggregate and group dataset (e.g. rowpipe reduce sales.csv 'total=sum(revenue)' --by country)")
  .option("--by <columns>", "Group by columns (comma-separated, e.g. country,category)")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (args, cmdOptions) => {
    let input = "-";
    let specs: string[] = [];

    if (args[0] && !args[0].includes("=")) {
      input = args[0];
      specs = args.slice(1);
    } else {
      specs = args;
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await reduceCommand(input, specs, opts);
  });

// 11. sample
program
  .command("sample [input]")
  .description("Reservoir sample rows with bounded memory")
  .option("--rows <number>", "Number of rows to sample", "1000")
  .option("--seed <number>", "Deterministic random seed")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await sampleCommand(input, opts);
  });

// 12. validate
program
  .command("validate [input]")
  .description("Validate stream against schema definition")
  .requiredOption("--schema <file>", "Path to JSON schema file")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .option("--json", "Output validation report as JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await validateCommand(input, opts);
  });

// 13. diff
program
  .command("diff <left> <right>")
  .description("Compare two tabular datasets by key and report row/column/schema differences")
  .requiredOption("--key <columns>", "Key column(s) for matching rows (comma-separated, e.g. id or country,user_id)")
  .option("--format <format>", "Output format: summary, rows, patch", "summary")
  .option("--only <type>", "Filter diff output to specific change type: added, removed, changed, unchanged")
  .option("--columns <columns>", "Only compare specified columns for changes (comma-separated)")
  .option("--ignore <columns>", "Ignore specific columns during comparison (comma-separated)")
  .option("--limit <number>", "Limit number of changed/added/removed rows output in rows/patch mode")
  .option("--json", "Output machine-readable diff summary JSON")
  .option("--output <file>", "Write diff output to file instead of stdout")
  .option("--fail-on-diff", "Exit with non-zero code if differences are found (CI mode)")
  .option("--duplicate-key <policy>", "Duplicate key policy: error, first, last", "error")
  .option("--coerce", "Coerce string numbers, booleans, and dates before comparing")
  .option("--epsilon <number>", "Numeric equality tolerance for floating point comparison", "0")
  .option("--ignore-case", "Case-insensitive string comparison")
  .option("--trim", "Trim whitespace from strings before comparison")
  .option("--memory-limit <size>", "Memory threshold before spilling index to disk (e.g. 256mb, 64mb, 1gb)", "256mb")
  .option("--sheet <sheet>", "Worksheet name for XLSX (applied to both left and right)")
  .option("--left-sheet <sheet>", "Worksheet name for left XLSX")
  .option("--right-sheet <sheet>", "Worksheet name for right XLSX")
  .option("--from <format>", "Input format for both datasets")
  .option("--from-left <format>", "Input format for left dataset")
  .option("--from-right <format>", "Input format for right dataset")
  .option("--no-schema", "Disable schema difference detection")
  .action(async (left, right, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await diffCommand(left, right, opts);
  });

// 14. files
program
  .command("files [args...]")
  .description("Stream files and directories as tabular records (e.g. rowpipe files ./src --include '*.ts')")
  .option("--no-recursive", "Disable recursive directory traversal")
  .option("--max-depth <number>", "Maximum directory recursion depth")
  .option("--include <glob>", "Glob pattern(s) to include (comma-separated or repeated)")
  .option("--exclude <glob>", "Glob pattern(s) to exclude (comma-separated or repeated)")
  .option("--hidden", "Include hidden files and directories (starting with '.')")
  .option("--follow-symlinks", "Follow symbolic links to directories")
  .option("--type <type>", "Filter record type: file, directory, symlink, all", "file")
  .option("--hash <algorithm>", "Compute streaming content hash: sha256, sha1, md5, fast")
  .option("--mime", "Infer MIME type and category from file extension")
  .option("-h, --human", "Format file size in human-readable units (e.g. 1.2 MB, 450 KB)")
  .option("--concurrency <number>", "Concurrency limit for stat and hash operations", "8")
  .option("--on-error <behavior>", "Error handling behavior: fail, skip", "fail")
  .option("--to <format>", "Output format: csv, tsv, psv, json, jsonl, xlsx, parquet, markdown")
  .option("--json", "Output file records as formatted JSON array")
  .option("--output <file>", "Write records to file instead of stdout")
  .option("--filter <expr>", "Filter rows using expression (e.g. 'size > 1000')")
  .option("--select <columns>", "Select columns (comma-separated)")
  .option("--sort <columns>", "Sort by column(s) (e.g. size:desc, name)")
  .option("--top <number>", "Extract top-K rows by sort column(s)")
  .option("--limit <number>", "Limit output to first N rows")
  .option("--offset <number>", "Skip first N rows")
  .option("--tail <number>", "Extract last N rows")
  .option("--unique [columns]", "Deduplicate rows by column(s)")
  .option("--group-by <columns>", "Group by columns")
  .option("--count", "Include count aggregation")
  .option("--sum <columns>", "Sum column(s)")
  .option("--avg <columns>", "Average column(s)")
  .option("--min <columns>", "Min column(s)")
  .option("--max <columns>", "Max column(s)")
  .option("--agg <specs>", "Compact aggregation spec (e.g. 'count(),sum(size)')")
  .option("--deterministic", "Produce deterministic output (normalized paths, omit volatile atime)")
  .action(async (args: string[], cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    const firstArg = args[0];

    if (firstArg === "snapshot") {
      const target = args[1] || ".";
      await filesSnapshotCommand(target, opts);
    } else if (firstArg === "diff") {
      const left = args[1];
      const right = args[2];
      if (!left || !right) {
        throw new RowpipeError("Usage: rowpipe files diff <left_dir> <right_dir>", 2);
      }
      await filesDiffCommand(left, right, opts);
    } else {
      const target = firstArg || ".";
      await filesCommand(target, opts);
    }
  });

// 15. diff-files (convenience alias)
program
  .command("diff-files <left> <right>")
  .description("Compare two directories as streaming tabular datasets")
  .option("--hash <algorithm>", "Compute and compare file hashes: sha256, sha1, md5, fast")
  .option("--format <format>", "Output format: summary, rows, patch", "summary")
  .option("--only <type>", "Filter diff output to specific change type: added, removed, changed, unchanged")
  .option("--json", "Output machine-readable diff summary JSON")
  .option("--fail-on-diff", "Exit with non-zero code if differences are found")
  .action(async (left, right, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await filesDiffCommand(left, right, opts);
  });

// 16. limit
program
  .command("limit [input] [count]")
  .description("Limit stream to first N rows with early upstream cancellation")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrCount, maybeCount, cmdOptions) => {
    let input = "-";
    let count = 10;
    if (maybeCount !== undefined) {
      input = inputOrCount || "-";
      count = Number(maybeCount) || 10;
    } else if (inputOrCount !== undefined && !Number.isNaN(Number(inputOrCount))) {
      count = Number(inputOrCount);
    } else if (inputOrCount !== undefined) {
      input = inputOrCount;
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await limitCommand(input, count, opts);
  });

// 17. offset
program
  .command("offset [input] [count]")
  .description("Skip first N rows in stream")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrCount, maybeCount, cmdOptions) => {
    let input = "-";
    let count = 0;
    if (maybeCount !== undefined) {
      input = inputOrCount || "-";
      count = Number(maybeCount) || 0;
    } else if (inputOrCount !== undefined && !Number.isNaN(Number(inputOrCount))) {
      count = Number(inputOrCount);
    } else if (inputOrCount !== undefined) {
      input = inputOrCount;
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await offsetCommand(input, count, opts);
  });

// 18. head
program
  .command("head [input] [lines]")
  .description("Emit first N rows of stream (default: 10)")
  .option("-n, --lines <number>", "Number of rows to emit", "10")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrLines, maybeLines, cmdOptions) => {
    let input = "-";
    let lines = cmdOptions.lines;
    if (maybeLines !== undefined) {
      input = inputOrLines || "-";
      lines = maybeLines;
    } else if (inputOrLines !== undefined && !Number.isNaN(Number(inputOrLines))) {
      lines = inputOrLines;
      input = "-";
    } else if (inputOrLines !== undefined) {
      input = inputOrLines;
    }
    const opts = { ...program.opts(), ...cmdOptions, lines: lines ?? cmdOptions.lines ?? "10" };
    await headCommand(input, opts);
  });

// 19. tail
program
  .command("tail [input] [lines]")
  .description("Emit last N rows of stream using bounded memory ring buffer (default: 10)")
  .option("-n, --lines <number>", "Number of rows to emit", "10")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrLines, maybeLines, cmdOptions) => {
    let input = "-";
    let lines = cmdOptions.lines;
    if (maybeLines !== undefined) {
      input = inputOrLines || "-";
      lines = maybeLines;
    } else if (inputOrLines !== undefined && !Number.isNaN(Number(inputOrLines))) {
      lines = inputOrLines;
      input = "-";
    } else if (inputOrLines !== undefined) {
      input = inputOrLines;
    }
    const opts = { ...program.opts(), ...cmdOptions, lines: lines ?? cmdOptions.lines ?? "10" };
    await tailCommand(input, opts);
  });

// 20. sort
program
  .command("sort [input] [specs]")
  .description("Sort stream by columns with typed comparisons and external merge sort on large files")
  .option("--by <specs>", "Sort columns (e.g. country,revenue:desc or age:desc)")
  .option("--nulls <placement>", "Null sorting placement: first or last", "last")
  .option("--ignore-case", "Case-insensitive string sorting")
  .option("--natural", "Natural numeric sorting for strings (e.g. file1, file2, file10)")
  .option("--memory-limit <size>", "Memory threshold before spilling to disk (e.g. 256mb, 64mb)", "256mb")
  .option("--temp-dir <dir>", "Custom temporary directory for disk spills")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (inputOrSpecs, maybeSpecs, cmdOptions) => {
    let input = "-";
    let by = cmdOptions.by;
    if (maybeSpecs !== undefined) {
      input = inputOrSpecs || "-";
      by = maybeSpecs;
    } else if (inputOrSpecs && (inputOrSpecs.includes(":") || (inputOrSpecs !== "-" && !existsSync(inputOrSpecs)))) {
      by = inputOrSpecs;
      input = "-";
    } else {
      input = inputOrSpecs || "-";
    }
    const opts = { ...program.opts(), ...cmdOptions, by: by || cmdOptions.by };
    await sortCommand(input, opts);
  });

// 21. top
program
  .command("top [input] [by] [lines]")
  .description("Retain top-N rows using bounded-memory binary heap (default: 10)")
  .option("--by <column>", "Sort column for ranking")
  .option("-n, --lines <number>", "Number of top rows to emit", "10")
  .option("--order <order>", "Sort order: asc or desc", "desc")
  .option("--smallest", "Emit smallest N rows (bottom-K) instead of largest")
  .option("--nulls <placement>", "Null placement: first or last", "last")
  .option("--ignore-case", "Case-insensitive string sorting")
  .option("--natural", "Natural numeric sorting")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (arg1, arg2, arg3, cmdOptions) => {
    let input = "-";
    let by = cmdOptions.by;
    let lines = cmdOptions.lines;

    if (arg3 !== undefined) {
      input = arg1 || "-";
      by = arg2;
      lines = arg3;
    } else if (arg2 !== undefined) {
      if (!Number.isNaN(Number(arg2))) {
        if (arg1 && (arg1 === "-" || existsSync(arg1))) {
          input = arg1;
          lines = arg2;
        } else {
          by = arg1;
          lines = arg2;
        }
      } else {
        input = arg1 || "-";
        by = arg2;
      }
    } else if (arg1 !== undefined) {
      if (!Number.isNaN(Number(arg1))) {
        lines = arg1;
      } else if (arg1 === "-" || existsSync(arg1)) {
        input = arg1;
      } else {
        by = arg1;
      }
    }

    const opts = { ...program.opts(), ...cmdOptions, by: by || cmdOptions.by, lines: lines || cmdOptions.lines || "10" };
    if (!opts.by) {
      throw new Error("Sort column is required for top command (e.g. rowpipe top netflix.csv revenue 10 or --by revenue)");
    }
    await topCommand(input, opts);
  });

// 22. unique
program
  .command("unique [input]")
  .description("Deduplicate rows by key columns (or whole row) with spillable memory management")
  .option("--by <columns>", "Key columns for uniqueness (comma-separated)")
  .option("--keep <policy>", "Keep first or last occurrence: first, last", "first")
  .option("--memory-limit <size>", "Memory limit before spilling keys to disk", "256mb")
  .option("--temp-dir <dir>", "Custom temporary directory for disk spills")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await uniqueCommand(input, opts);
  });

// 23. count
program
  .command("count [input]")
  .description("Fast streaming row count, distinct count, or group-by counts")
  .option("--by <columns>", "Group by columns for group-counts (comma-separated)")
  .option("--distinct <column>", "Count distinct values of a column")
  .option("--approx", "Use HyperLogLog for approximate distinct count (O(1) memory)")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await countCommand(input, opts);
  });

// 24. group
program
  .command("group [input]")
  .description("Group dataset and compute aggregations (e.g. rowpipe group sales.csv --by country --sum revenue)")
  .option("--by <columns>", "Group by columns (comma-separated)")
  .option("--count", "Aggregate count of rows per group")
  .option("--sum <columns>", "Sum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--avg <columns>", "Average column values", (val, prev: string[] = []) => [...prev, val])
  .option("--min <columns>", "Minimum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--max <columns>", "Maximum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--first <columns>", "First column values", (val, prev: string[] = []) => [...prev, val])
  .option("--last <columns>", "Last column values", (val, prev: string[] = []) => [...prev, val])
  .option("--agg <specs>", "Aggregation specifications", (val, prev: string[] = []) => [...prev, val])
  .option("--memory-limit <size>", "Memory threshold before spilling groups to disk", "256mb")
  .option("--temp-dir <dir>", "Custom temporary directory for disk spills")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await groupCommand(input, opts);
  });

// 25. db
program
  .command("db <url>")
  .description("Stream tabular data directly from or to PostgreSQL, MySQL, and SQLite databases")
  .option("--table <table>", "Read from database table")
  .option("--query <sql>", "Execute raw SQL query")
  .option("--params <json>", "Query parameters as JSON array (e.g. '[100, true]')")
  .option("--param <val>", "Single query parameter (can be specified multiple times)", (val, prev: string[] = []) => [...prev, val])
  .option("--where <sql>", "Database WHERE clause shortcut")
  .option("--tables", "List all tables in the database")
  .option("--schema [table]", "Inspect database table schema")
  .option("--to-db <url>", "Write stream to target database URL")
  .option("--to-table <table>", "Target database table name")
  .option("--create-table", "Automatically create target table from schema if not exists")
  .option("--upsert", "Perform upsert (insert or update on conflict)")
  .option("--conflict <columns>", "Conflict columns for upsert (comma-separated)")
  .option("--transaction", "Wrap batch writes in a database transaction", true)
  .option("--no-transaction", "Disable database transactions for writes")
  .option("--truncate", "Truncate target table before writing (destructive)")
  .option("--dry-run", "Print generated DDL/SQL without executing")
  .option("--filter <expression>", "Filter rows using expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Project subset of columns (comma-separated)")
  .option("--rename <specs...>", "Rename columns (old=new)")
  .option("--cast <specs...>", "Cast column types (col:type)")
  .option("--map <specs...>", "Derive new columns (col=expr)")
  .option("--sort <specs>", "Sort stream by columns (--sort country,revenue:desc)")
  .option("--top <number>", "Retain top-N rows using bounded heap")
  .option("--limit <number>", "Limit number of rows emitted")
  .option("--offset <number>", "Skip first N rows")
  .option("--tail <number>", "Emit last N rows")
  .option("--unique [columns]", "Deduplicate rows")
  .option("--group-by <columns>", "Group by columns")
  .option("--count", "Aggregate count of rows")
  .option("--sum <columns>", "Sum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--avg <columns>", "Average column values", (val, prev: string[] = []) => [...prev, val])
  .option("--min <columns>", "Minimum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--max <columns>", "Maximum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--to <format>", "Output format (csv, json, jsonl, xlsx, markdown, etc.)")
  .option("--output <file>", "Write output to file")
  .option("--batch-size <number>", "Batch size for database reading and writing", "1000")
  .option("--json", "Output machine-readable JSON")
  .option("--quiet", "Suppress non-data output")
  .option("--no-progress", "Disable progress bar")
  .action(async (url, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await dbCommand(url, opts);
  });

// 26. join
program
  .command("join <left> <right>")
  .description("Join two tabular streams with bounded memory and spill-to-disk hash join")
  .option("--on <specs...>", "Join key column(s) or equality spec (e.g. id or user_id=id)", (val, prev: string[] = []) => [...prev, val])
  .option("--left-key <columns>", "Left join key column(s)")
  .option("--right-key <columns>", "Right join key column(s)")
  .option("--type <type>", "Join type: inner, left, right, full, semi, anti", "inner")
  .option("--prefix-left <prefix>", "Prefix for left columns")
  .option("--suffix-left <suffix>", "Suffix for left columns")
  .option("--prefix-right <prefix>", "Prefix for right columns")
  .option("--suffix-right <suffix>", "Suffix for colliding right columns", "_right")
  .option("--memory-limit <size>", "Memory threshold before spilling right table to disk", "256mb")
  .option("--temp-dir <dir>", "Custom temporary directory for disk spills")
  .option("--from-left <format>", "Input format for left dataset")
  .option("--from-right <format>", "Input format for right dataset")
  .option("--from <format>", "Input format for both datasets")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .option("--batch-size <number>", "Batch size in rows", "1000")
  .option("--filter <expression>", "Filter rows using expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Project subset of columns (comma-separated)")
  .option("--rename <specs...>", "Rename columns (old=new)")
  .option("--cast <specs...>", "Cast column types (col:type)")
  .option("--sort <specs>", "Sort stream by columns")
  .option("--top <number>", "Retain top-N rows")
  .option("--limit <number>", "Limit number of rows emitted")
  .option("--quiet", "Suppress non-data output")
  .option("--no-progress", "Disable progress bar")
  .action(async (left, right, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await joinCommand(left, right, opts);
  });

// 27. window
program
  .command("window [input]")
  .description("Apply sliding window and rolling calculations over stream (e.g. row_number(), lag(), moving_avg())")
  .option("--spec <specs...>", "Window function specifications (e.g. 'rn=row_number()', 'prev=lag(revenue)')", (val, prev: string[] = []) => [...prev, val])
  .option("--by <columns>", "Partition by columns (comma-separated)")
  .option("--partition-by <columns>", "Partition by columns (comma-separated)")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .option("--batch-size <number>", "Batch size in rows", "1000")
  .option("--filter <expression>", "Filter rows using expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Project subset of columns (comma-separated)")
  .option("--sort <specs>", "Sort stream by columns")
  .option("--limit <number>", "Limit number of rows emitted")
  .option("--quiet", "Suppress non-data output")
  .option("--no-progress", "Disable progress bar")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await windowCommand(input, opts);
  });

// 28. profile
program
  .command("profile [input]")
  .description("Profile dataset quality, null distributions, cardinality, numeric stats, and type anomalies")
  .option("--sample <number>", "Limit profile analysis to sample size")
  .option("--json", "Output machine-readable JSON profile")
  .option("--markdown", "Output GitHub Flavored Markdown profile report")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom CSV delimiter")
  .option("--quiet", "Suppress progress bar")
  .option("--no-progress", "Disable progress bar")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await profileCommand(input, opts);
  });

// 29. clean
program
  .command("clean [input]")
  .description("Clean and sanitize dataset (trim whitespace, map nulls, fill defaults, coerce types)")
  .option("--trim", "Trim leading/trailing whitespace from string fields", true)
  .option("--no-trim", "Disable whitespace trimming")
  .option("--null-values <list>", "Comma-separated list of string representations to map to null (e.g. 'N/A,NA,null,-')")
  .option("--fill-nulls <specs...>", "Fill null values with default (e.g. '0' or 'revenue=0,country=Unknown')")
  .option("--coerce", "Auto coerce string fields to numeric, boolean, and date types")
  .option("--dedup [columns]", "Deduplicate rows by column(s) or entire row")
  .option("--case <type>", "Normalize string casing: lower, upper, title")
  .option("--strip-chars <chars>", "Strip unwanted characters from string fields")
  .option("--drop-empty-rows", "Drop rows where all fields are null or empty")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .option("--quiet", "Suppress non-data output")
  .option("--no-progress", "Disable progress bar")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await cleanCommand(input, opts);
  });

// 30. explain
program
  .command("explain [args...]")
  .description("Visualize execution plan, optimizations (e.g. sort+limit -> top-k, database pushdown), and memory safety")
  .option("--table <table>", "Database table name")
  .option("--query <sql>", "Database raw SQL query")
  .option("--where <sql>", "Database WHERE clause")
  .option("--to-db <url>", "Target database URL")
  .option("--to-table <table>", "Target database table")
  .option("--filter <expression>", "Filter expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Column projection")
  .option("--sort <specs>", "Sort specification")
  .option("--top <number>", "Top-N count")
  .option("--limit <number>", "Row limit")
  .option("--offset <number>", "Row offset")
  .option("--tail <number>", "Tail count")
  .option("--unique [columns]", "Unique deduplication")
  .option("--group-by <columns>", "Group by columns")
  .option("--count", "Group count")
  .option("--sum <columns>", "Sum column", (val, prev: string[] = []) => [...prev, val])
  .option("--avg <columns>", "Average column", (val, prev: string[] = []) => [...prev, val])
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .action(async (args, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await explainCommand(args && args.length > 0 ? args : "-", opts);
  });

// 31. view
program
  .command("view [input]")
  .description("Interactive terminal data viewer with scrolling, live searching, sorting, and bounded memory")
  .option("--title <title>", "Custom title for viewer header bar")
  .option("--from <format>", "Input format (csv, tsv, json, jsonl, parquet, xlsx, sqlite, postgres, mysql)")
  .option("--sheet <sheet>", "Worksheet name or index for XLSX")
  .option("--table <table>", "Database table name")
  .option("--delimiter <delim>", "Custom delimiter for CSV/TSV")
  .option("--max-buffer <number>", "Maximum in-memory cached rows window", "25000")
  .option("--initial-rows <number>", "Initial rows to preload into buffer", "500")
  .option("--filter <expression>", "Initial filter expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Initial column projection")
  .option("--no-interactive", "Output static terminal table without entering interactive fullscreen mode")
  .option("--gzip", "Use GZIP decompression")
  .option("--brotli", "Use Brotli decompression")
  .option("--zstd", "Use Zstandard decompression")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await viewCommand(input, opts);
  });

// 32. explode
program
  .command("explode [input] [column]")
  .description("Expand array or delimited string column into multiple rows (1 -> N)")
  .option("--delimiter <delim>", "Delimiter for splitting strings (default: ',')")
  .option("--no-trim", "Do not trim whitespace around split values")
  .option("--preserve-empty", "Preserve empty string elements")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .option("--quiet", "Suppress non-data output")
  .option("--no-progress", "Disable progress bar")
  .action(async (inputOrCol, maybeCol, cmdOptions) => {
    let input = "-";
    let column = "";
    if (maybeCol !== undefined) {
      input = inputOrCol || "-";
      column = maybeCol;
    } else {
      column = inputOrCol || "";
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await explodeCommand(input, column, opts);
  });

// 33. flatten
program
  .command("flatten [input]")
  .description("Flatten nested JSON objects into dot-notated columns")
  .option("--separator <sep>", "Field separator for nested paths", ".")
  .option("--max-depth <depth>", "Maximum recursion depth", "10")
  .option("--arrays", "Flatten nested array indices as well")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .option("--quiet", "Suppress non-data output")
  .option("--no-progress", "Disable progress bar")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await flattenCommand(input, opts);
  });

// 34. freq
program
  .command("freq [input] [column]")
  .description("Compute frequency distribution and value counts for a column")
  .option("-n, --top <number>", "Number of top frequent values to show", "20")
  .option("-c, --chart", "Render horizontal bar chart for frequencies")
  .option("--width <number>", "Maximum bar chart width in characters", "30")
  .option("--asc", "Sort by frequency ascending instead of descending")
  .option("--no-nulls", "Exclude null/empty values from count")
  .option("--json", "Output frequency table as JSON")
  .option("--to <format>", "Output format (csv, json, jsonl, markdown, table)")
  .option("--from <format>", "Input format")
  .option("--output <file>", "Write output to file")
  .action(async (inputOrCol, maybeCol, cmdOptions) => {
    let input = "-";
    let column = "";
    if (maybeCol !== undefined) {
      input = inputOrCol || "-";
      column = maybeCol;
    } else {
      column = inputOrCol || "";
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await freqCommand(input, column, opts);
  });

// 35. plot / chart
program
  .command("plot [input] [col1] [col2]")
  .alias("chart")
  .description("Render horizontal ASCII/Unicode bar charts and numeric histograms")
  .option("-n, --top <number>", "Number of top categories to show", "20")
  .option("--bins <number>", "Number of bins for numeric histogram", "10")
  .option("--width <number>", "Maximum bar chart width in characters", "30")
  .option("--numeric", "Force numeric histogram rendering")
  .option("--categorical", "Force categorical frequency bar chart")
  .option("--asc", "Sort ascending instead of descending")
  .option("--no-nulls", "Exclude null/empty values")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom delimiter for CSV/TSV")
  .action(async (inputOrCol1, maybeCol1, maybeCol2, cmdOptions) => {
    let input = "-";
    let col1: string | undefined;
    let col2: string | undefined;

    if (maybeCol2 !== undefined) {
      input = inputOrCol1 || "-";
      col1 = maybeCol1;
      col2 = maybeCol2;
    } else if (maybeCol1 !== undefined) {
      if (existsSync(inputOrCol1) || inputOrCol1.includes("/") || inputOrCol1.includes(".")) {
        input = inputOrCol1;
        col1 = maybeCol1;
      } else {
        input = "-";
        col1 = inputOrCol1;
        col2 = maybeCol1;
      }
    } else {
      input = "-";
      col1 = inputOrCol1;
    }

    const opts = { ...program.opts(), ...cmdOptions };
    await plotCommand(input, col1, col2, opts);
  });

// 36. table
program
  .command("table [input]")
  .description("Render dataset as pretty terminal grid table with Unicode/ASCII box drawing")
  .option("--style <style>", "Box border style (unicode, ascii, compact)", "unicode")
  .option("--max-width <number>", "Maximum column width before truncation", "40")
  .option("--max-buffer <number>", "Maximum rows to buffer for alignment", "5000")
  .option("--filter <expression>", "Filter rows using expression", (val, prev: string[] = []) => [...prev, val])
  .option("--select <columns>", "Project subset of columns")
  .option("--sort <specs>", "Sort stream by columns")
  .option("--limit <number>", "Limit number of rows emitted")
  .option("--offset <number>", "Skip first N rows")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom delimiter for CSV/TSV")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await tableCommand(input, opts);
  });

// 37. completion
program
  .command("completion [shell]")
  .description("Generate shell autocompletion script (zsh, bash, fish)")
  .action(async (shell = "zsh", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await completionCommand(shell, opts);
  });

// 38. partition
program
  .command("partition [input]")
  .description("Partition dataset into dynamic files/directories based on column values")
  .option("--by <columns>", "Partition columns (e.g. country,release_year)")
  .option("-o, --out-pattern <pattern>", "Output file path template (e.g. 'dist/{country}/{release_year}.csv')")
  .option("--to <format>", "Output format (csv, json, jsonl, parquet)")
  .option("--max-open <number>", "Maximum open file descriptors pool", "50")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom delimiter for CSV/TSV")
  .option("--quiet", "Suppress summary output")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await partitionCommand(input, opts);
  });

// 39. split
program
  .command("split [input]")
  .description("Split stream into sequential chunk files based on row count")
  .option("-n, --chunk-size <number>", "Number of rows per chunk file", "100000")
  .option("-o, --out-pattern <pattern>", "Output file path template (e.g. 'parts/chunk_{n:03d}.csv')")
  .option("--to <format>", "Output format (csv, json, jsonl, parquet)")
  .option("--from <format>", "Input format")
  .option("--sheet <sheet>", "Worksheet name for XLSX")
  .option("--delimiter <delim>", "Custom delimiter for CSV/TSV")
  .option("--quiet", "Suppress summary output")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await splitCommand(input, opts);
  });

// 40. timeseries
program
  .command("timeseries [input]")
  .description("Resample timeseries data into fixed time buckets and fill missing gaps")
  .option("-t, --time-col <col>", "Time/timestamp column name")
  .option("-i, --interval <window>", "Time interval window (e.g. 5s, 1m, 15m, 1h, 1d, 1w, 1mo, 1y)", "1m")
  .option("--agg <specs...>", "Aggregation specifications (e.g. avg(cpu),max(mem))", (val, prev: string[] = []) => [...prev, val])
  .option("--avg <columns>", "Average column values", (val, prev: string[] = []) => [...prev, val])
  .option("--sum <columns>", "Sum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--min <columns>", "Minimum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--max <columns>", "Maximum column values", (val, prev: string[] = []) => [...prev, val])
  .option("--first <columns>", "First column values", (val, prev: string[] = []) => [...prev, val])
  .option("--last <columns>", "Last column values", (val, prev: string[] = []) => [...prev, val])
  .option("--count", "Include count of records per time bucket")
  .option("--fill-gaps <method>", "Gap filling method: ffill, bfill, zero, null, linear")
  .option("--time-format <format>", "Output timestamp format: iso, epoch_ms, epoch_s, date_only", "iso")
  .option("--to <format>", "Output format (csv, json, jsonl, parquet, markdown, table)")
  .option("--from <format>", "Input format")
  .option("--output <file>", "Write output to file")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await timeseriesCommand(input, opts);
  });

// 41. corr
program
  .command("corr [input]")
  .description("Compute Pearson correlation matrix across numeric columns")
  .option("--cols <columns>", "Target columns (comma-separated)")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format (csv, json, jsonl, markdown, table)")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await corrCommand(input, opts);
  });

// 42. quantiles / percentiles
program
  .command("quantiles [input] [column]")
  .alias("percentiles")
  .description("Compute streaming percentiles (P50, P90, P95, P99, P99.9, IQR, etc.)")
  .option("-p, --p <percentiles>", "Percentiles to calculate (e.g. 50,90,95,99)", "25,50,75,90,95,99")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (inputOrCol, maybeCol, cmdOptions) => {
    let input = "-";
    let column = "";
    if (maybeCol !== undefined) {
      input = inputOrCol || "-";
      column = maybeCol;
    } else {
      column = inputOrCol || "";
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await quantilesCommand(input, column, opts);
  });

// 43. outliers / anomalies
program
  .command("outliers [input] [column]")
  .alias("anomalies")
  .description("Detect and filter statistical outliers and anomalies using Z-Score, IQR, or MAD")
  .option("-c, --col <column>", "Target column name")
  .option("-m, --method <method>", "Method: zscore, iqr, mad", "zscore")
  .option("--threshold <number>", "Statistical threshold (default: 3.0 for zscore/mad, 1.5 for iqr)")
  .option("--only-outliers", "Filter stream to output only outlier rows")
  .option("--add-columns", "Append _is_outlier and _outlier_score columns")
  .option("--invert", "Emit only clean inlier rows")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (inputOrCol, maybeCol, cmdOptions) => {
    let input = "-";
    let column = "";
    if (maybeCol !== undefined) {
      input = inputOrCol || "-";
      column = maybeCol;
    } else if (cmdOptions.col || cmdOptions.column) {
      input = inputOrCol || "-";
      column = cmdOptions.col || cmdOptions.column;
    } else {
      column = inputOrCol || "";
    }
    const opts = { ...program.opts(), ...cmdOptions, col: column || cmdOptions.col };
    await outliersCommand(input, opts);
  });

// 44. crosstab
program
  .command("crosstab [input] [rowCol] [colCol]")
  .description("Compute 2D contingency table and Chi-Square independence test")
  .option("-r, --row <col>", "Row dimension column")
  .option("-c, --col <col>", "Column dimension column")
  .option("-n, --top <number>", "Limit top categories per dimension", "15")
  .option("--normalize <method>", "Normalization: row, col, all")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (arg1, arg2, arg3, cmdOptions) => {
    let input = "-";
    let rowCol = "";
    let colCol = "";
    if (arg3 !== undefined) {
      input = arg1 || "-";
      rowCol = arg2 || "";
      colCol = arg3 || "";
    } else {
      rowCol = arg1 || "";
      colCol = arg2 || "";
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await crosstabCommand(input, rowCol, colCol, opts);
  });

// 45. regression / trend
program
  .command("regression [input] [xCol] [yCol]")
  .alias("trend")
  .description("Compute Ordinary Least Squares (OLS) Linear Regression model")
  .option("-x, --x <column>", "Independent variable column (X)")
  .option("-y, --y <column>", "Dependent variable column (Y)")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (arg1, arg2, arg3, cmdOptions) => {
    let input = "-";
    let xCol = "";
    let yCol = "";
    if (arg3 !== undefined) {
      input = arg1 || "-";
      xCol = arg2 || "";
      yCol = arg3 || "";
    } else {
      xCol = arg1 || "";
      yCol = arg2 || "";
    }
    const opts = { ...program.opts(), ...cmdOptions };
    await regressionCommand(input, xCol, yCol, opts);
  });

// 46. rfm
program
  .command("rfm [input]")
  .description("Compute RFM (Recency, Frequency, Monetary) customer segmentation and scoring")
  .option("-c, --customer-id <col>", "Customer / user ID column")
  .option("-d, --date-col <col>", "Transaction / order date column")
  .option("-a, --amount-col <col>", "Monetary amount / revenue column")
  .option("--as-of <date>", "Reference date for recency calculation (default: latest date in stream)")
  .option("--summary", "Output aggregated segment breakdown and statistics")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await rfmCommand(input, opts);
  });

// 47. cohort
program
  .command("cohort [input]")
  .description("Compute user retention cohort matrix over time intervals")
  .option("-u, --user-id <col>", "User / customer ID column")
  .option("-t, --time-col <col>", "Timestamp / event date column")
  .option("-i, --interval <interval>", "Cohort interval: 1d, 1w, 1mo, 1y", "1mo")
  .option("--counts", "Display raw user retention counts instead of percentages")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await cohortCommand(input, opts);
  });

// 48. funnel
program
  .command("funnel [input]")
  .description("Compute conversion funnel, step drop-off rates, and visual ASCII funnel")
  .option("-s, --steps <steps>", "Comma-separated list of funnel stages (e.g. 'view,cart,checkout,purchase')")
  .option("-u, --user-id <col>", "User / session ID column")
  .option("--step-col <col>", "Step / event name column")
  .option("-t, --time-col <col>", "Event timestamp column")
  .option("--strict", "Enforce strict chronological step order per user")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await funnelCommand(input, opts);
  });

// 49. abtest
program
  .command("abtest [input]")
  .description("Compute A/B test statistical significance (Two-Proportion Z-Test or Welch's T-Test)")
  .option("-g, --group <col>", "Experiment group / variant column")
  .option("-m, --metric <col>", "Conversion metric (0/1) or numeric value column")
  .option("--control <group>", "Control group name (default: auto-detected 'control' or 'A')")
  .option("--variant <group>", "Variant group name (default: auto-detected 'variant' or 'B')")
  .option("--type <type>", "Test type: proportion, means, auto", "auto")
  .option("--confidence-level <level>", "Confidence level (e.g. 0.95, 0.99, 0.90)", "0.95")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await abTestCommand(input, opts);
  });

// 50. pareto
program
  .command("pareto [input]")
  .description("Compute 80/20 Pareto distribution and ABC inventory/revenue classification")
  .option("-i, --item <col>", "Item / SKU / category / customer column")
  .option("-v, --value <col>", "Value / revenue / sales / quantity column")
  .option("--a-threshold <pct>", "Threshold percentage for Class A (default: 80)", "80")
  .option("--b-threshold <pct>", "Threshold percentage for Class B (default: 95)", "95")
  .option("--summary", "Output aggregated ABC class summary breakdown")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await paretoCommand(input, opts);
  });

// 51. technical / indicators
program
  .command("technical [input]")
  .alias("indicators")
  .description("Compute financial technical indicators (SMA, EMA, RSI, MACD, Bollinger Bands, VWAP, ATR)")
  .option("-p, --price <col>", "Price / close price column")
  .option("-v, --volume <col>", "Volume column")
  .option("--high <col>", "High price column")
  .option("--low <col>", "Low price column")
  .option("--close <col>", "Close price column")
  .option("-i, --indicators <specs>", "Indicator list (e.g. 'sma(20),rsi(14),macd,bollinger(20,2),vwap')")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await technicalCommand(input, opts);
  });

// 52. cluster / kmeans
program
  .command("cluster [input]")
  .alias("kmeans")
  .description("Perform O(1) streaming Mini-Batch K-Means clustering across numeric features")
  .option("-k, --k <number>", "Number of clusters (default: 3)", "3")
  .option("-c, --cols <columns>", "Target numeric feature columns (comma-separated)")
  .option("--summary", "Output cluster centroids summary instead of row assignments")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await clusterCommand(input, opts);
  });

// 53. entropy / importance
program
  .command("entropy [input]")
  .alias("importance")
  .description("Compute Shannon entropy, mutual information, and feature importance rankings")
  .option("-t, --target <col>", "Target / label column name")
  .option("-c, --cols <columns>", "Feature columns to evaluate (comma-separated)")
  .option("--bins <number>", "Numeric discretization bins count (default: 10)", "10")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await entropyCommand(input, opts);
  });

// 54. ngrams / tokens
program
  .command("ngrams [input] [column]")
  .alias("tokens")
  .description("Extract N-Gram word phrases (unigrams, bigrams, trigrams) and frequency counts")
  .option("-c, --col <column>", "Target text column name")
  .option("-n, --n <number>", "N-Gram size: 1 (unigram), 2 (bigram), 3 (trigram)", "2")
  .option("--top <number>", "Limit top frequent N-Grams (default: 20)", "20")
  .option("--stopwords", "Filter out common English stopwords")
  .option("--min-freq <number>", "Minimum frequency threshold", "1")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (inputOrCol, maybeCol, cmdOptions) => {
    let input = "-";
    let column = "";
    if (maybeCol !== undefined) {
      input = inputOrCol || "-";
      column = maybeCol;
    } else if (cmdOptions.col) {
      input = inputOrCol || "-";
      column = cmdOptions.col;
    } else {
      column = inputOrCol || "";
    }
    const opts = { ...program.opts(), ...cmdOptions, col: column || cmdOptions.col };
    await ngramsCommand(input, column, opts);
  });

// 55. pivot / pivot-table / crosstab-pivot
program
  .command("pivot [input]")
  .aliases(["pivot-table", "crosstab-pivot"])
  .description("Create multi-dimensional pivot table with aggregated metrics across row and column dimensions")
  .option("-i, --index <cols>", "Index / row grouping column(s) (comma-separated)")
  .option("-c, --columns <col>", "Column whose unique values become output columns")
  .option("-v, --values <col>", "Metric / value column to aggregate")
  .option("-a, --agg <func>", "Aggregation function: sum, avg, min, max, count, first, last", "sum")
  .option("--fill <val>", "Fill value for empty cells")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await pivotCommand(input, opts);
  });

// 56. unpivot / melt / wide-to-long
program
  .command("unpivot [input]")
  .aliases(["melt", "wide-to-long"])
  .description("Unpivot / melt wide dataset into long key-value format (O(1) memory)")
  .option("-i, --index <cols>", "Index / identifier column(s) to keep fixed (comma-separated)")
  .option("-c, --columns <cols>", "Value columns to unpivot (comma-separated, defaults to all non-index columns)")
  .option("--var-col <name>", "Name for variable / dimension column", "variable")
  .option("--val-col <name>", "Name for metric / value column", "value")
  .option("--drop-null", "Drop rows where unpivoted value is null")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await unpivotCommand(input, opts);
  });

// 57. fuzzy-join / fuzzyjoin / fuzzy
program
  .command("fuzzy-join <left> <right>")
  .aliases(["fuzzyjoin", "fuzzy"])
  .description("Fuzzy approximate string matching join (Levenshtein, Jaro-Winkler, Jaccard, Soundex)")
  .option("-o, --on <col>", "Key column name to compare in both datasets")
  .option("--left-key <col>", "Left dataset key column")
  .option("--right-key <col>", "Right dataset key column")
  .option("--type <type>", "Join type: inner, left, right, full", "left")
  .option("-m, --method <algo>", "Similarity algorithm: levenshtein, jaro-winkler, jaccard, soundex", "levenshtein")
  .option("-t, --threshold <num>", "Match threshold between 0.0 and 1.0 (default: 0.75)", "0.75")
  .option("--all-matches", "Keep all matching right records instead of only best match")
  .option("--score-col <col>", "Add similarity score column (e.g. 'similarity_score')")
  .option("--from-left <fmt>", "Format of left file")
  .option("--from-right <fmt>", "Format of right file")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (left, right, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions, bestMatch: !cmdOptions.allMatches };
    await fuzzyJoinCommand(left, right, opts);
  });

// 58. concat / stack / union-all
program
  .command("concat <files...>")
  .aliases(["stack", "union-all"])
  .description("Stream concatenate multiple tabular files sequentially with unified schema")
  .option("-s, --source-col <name>", "Add source filename / path column to rows")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (files, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await concatCommand(files, opts);
  });

// 59. generate / mock / fake / synth
program
  .command("generate [schema]")
  .aliases(["mock", "fake", "synth"])
  .description("Stream generate synthetic tabular data based on column generators (seq, uuid, name, email, int, float, date, choice)")
  .option("-n, --rows <number>", "Number of rows to generate (default: 1000)", "1000")
  .option("-s, --schema <spec>", "Column schema specifications (e.g. 'id:seq,name:name,email:email,age:int(18,70)')")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (schema, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await generateCommand(schema, opts);
  });

// 60. mask / anonymize / redact
program
  .command("mask [input]")
  .aliases(["anonymize", "redact"])
  .description("Redact and mask sensitive PII fields (email, phone, credit card, IP, name, hash)")
  .option("--email <cols>", "Email columns to mask (e.g. 'j***@domain.com')")
  .option("--phone <cols>", "Phone columns to mask (e.g. '***-***-1234')")
  .option("--card <cols>", "Credit card columns to mask (e.g. '**** **** **** 1234')")
  .option("--ip <cols>", "IP columns to mask (e.g. '192.168.***.***')")
  .option("--name <cols>", "Name columns to mask (e.g. 'J*** D***')")
  .option("--hash <cols>", "Columns to tokenize with deterministic SHA-256 hash")
  .option("--redact <cols>", "Columns to redact completely with [REDACTED]")
  .option("--salt <salt>", "Salt for HMAC/SHA-256 hashing")
  .option("--from <format>", "Input format")
  .option("--to <format>", "Output format")
  .option("--output <file>", "Write output to file")
  .option("--json", "Output machine-readable JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await maskCommand(input, opts);
  });

// 61. test / assert / check
program
  .command("test [input]")
  .aliases(["assert", "check"])
  .description("Run streaming data quality assertions and constraints (CI/CD assert runner)")
  .option("-a, --assert <expressions...>", "Expressions that must evaluate truthy on every row")
  .option("--not-null <cols>", "Columns that must not contain null/empty values (comma-separated)")
  .option("--unique <cols>", "Columns that must contain strictly unique values (comma-separated)")
  .option("--min-rows <number>", "Minimum row count requirement")
  .option("--max-rows <number>", "Maximum row count limit")
  .option("--max-errors <number>", "Limit number of sample violation rows reported", "5")
  .option("--fail-fast", "Stop processing stream on first assertion failure")
  .option("--from <format>", "Input format")
  .option("--json", "Output machine-readable test summary JSON")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await testCommand(input, opts);
  });

// 62. serve / api / http
program
  .command("serve [input]")
  .aliases(["api", "http"])
  .description("Instantly serve dataset via zero-dependency streaming HTTP REST API")
  .option("-p, --port <number>", "Port to bind HTTP server (default: 3000)", "3000")
  .option("-h, --host <host>", "Host address to bind (default: 0.0.0.0)", "0.0.0.0")
  .option("--from <format>", "Input format")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    await serveCommand(input, opts);
  });

// 63. report / summary-html
program
  .command("report [input]")
  .aliases(["summary-html"])
  .description("Generate modern offline self-contained HTML data health dashboard and report")
  .option("-t, --title <title>", "Report title")
  .option("-o, --output <file>", "Output HTML file path")
  .option("--from <format>", "Input format")
  .action(async (input = "-", cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    if (!opts.output) {
      opts.output = "report.html";
    }
    await reportCommand(input, opts);
  });

// 64. fetch / http-get / curl-stream
program
  .command("fetch <url>")
  .aliases(["http-get", "curl-stream"])
  .description("Stream tabular data from remote REST API with pagination and data path extraction")
  .option("-H, --header <key:val...>", "Custom HTTP headers", (val, prev: string[] = []) => [...prev, val])
  .option("--bearer <token>", "Bearer authorization token")
  .option("--auth <user:pass>", "Basic authorization credentials")
  .option("--data-path <path>", "Dot-path to extract array from JSON response envelope (e.g. data.items)")
  .option("--paginate <mode>", "Pagination mode: page, offset, cursor (default: none)", "none")
  .option("--page-param <param>", "Query parameter for page number (default: page)", "page")
  .option("--offset-param <param>", "Query parameter for offset (default: offset)", "offset")
  .option("--limit-param <param>", "Query parameter for page size (default: limit)", "limit")
  .option("--page-size <number>", "Page size limit per request")
  .option("--cursor-param <param>", "Query parameter for cursor token", "cursor")
  .option("--cursor-path <path>", "Dot-path in response JSON containing next cursor token")
  .option("--max-pages <number>", "Maximum number of pages to fetch (default: 100)", "100")
  .action(async (url, cmdOptions) => {
    const opts = { ...program.opts(), ...cmdOptions };
    if (cmdOptions.header) {
      opts.headersList = Array.isArray(cmdOptions.header) ? cmdOptions.header : [cmdOptions.header];
    }
    if (cmdOptions.pageSize) opts.pageSize = parseInt(cmdOptions.pageSize, 10);
    if (cmdOptions.maxPages) opts.maxPages = parseInt(cmdOptions.maxPages, 10);
    await fetchCommand(url, opts);
  });

async function main() {
  if (process.argv.length <= 2 && process.stdin.isTTY) {
    program.outputHelp();
    return;
  }
  try {
    await program.parseAsync(process.argv);
  } catch (err: unknown) {
    if (err instanceof RowpipeError) {
      process.stderr.write(`\nError: ${err.message}\n`);
      process.exit(err.exitCode);
    }
    process.stderr.write(`\nUnexpected error: ${(err as Error).message}\n`);
    if (process.env["DEBUG"]) {
      process.stderr.write(`${(err as Error).stack}\n`);
    }
    process.exit(1);
  }
}

main();
