import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { existsSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { execSync } from "node:child_process";
import { DatabaseReader } from "../src/db/source.js";
import { DatabaseWriter } from "../src/db/sink.js";
import { SqliteDatabaseAdapter } from "../src/db/adapters/sqlite.js";
import { parseDatabaseUrl, sanitizeConnectionString } from "../src/db/url.js";
import { analyzeDatabasePushdown } from "../src/db/pushdown.js";
import { generateCreateTableSql, rowpipeTypeToSqlite, rowpipeTypeToPostgres, rowpipeTypeToMysql } from "../src/db/mapping.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { DataBatch, Row, Schema } from "../src/core/types.js";

const TEST_DIR = join(process.cwd(), "scratch_test_db");
const DB_FILE = join(TEST_DIR, "test.db");
const TARGET_DB_FILE = join(TEST_DIR, "target.db");
const CLI_PATH = join(process.cwd(), "dist/cli/index.js");

describe("Database Streaming & Adapter Test Suite", () => {
  beforeEach(() => {
    if (!existsSync(TEST_DIR)) mkdirSync(TEST_DIR, { recursive: true });
  });

  afterEach(() => {
    if (existsSync(TEST_DIR)) rmSync(TEST_DIR, { recursive: true, force: true });
  });

  // 1. Connection URL Sanitization & Parsing
  describe("Security & Connection URL Sanitization", () => {
    it("should sanitize PostgreSQL passwords from connection URLs", () => {
      const url = "postgres://admin:SuperSecret123@db.example.com:5432/production_app";
      const sanitized = sanitizeConnectionString(url);
      expect(sanitized).toBe("postgres://admin:***@db.example.com:5432/production_app");
      expect(sanitized).not.toContain("SuperSecret123");
    });

    it("should sanitize MySQL passwords from connection URLs", () => {
      const url = "mysql://root:p@ssw0rd!@127.0.0.1:3306/analytics_db";
      const sanitized = sanitizeConnectionString(url);
      expect(sanitized).toBe("mysql://root:***@127.0.0.1:3306/analytics_db");
      expect(sanitized).not.toContain("p@ssw0rd!");
    });

    it("should parse SQLite file URLs and parameters", () => {
      const cfg = parseDatabaseUrl("sqlite://./data.db?table=users");
      expect(cfg.dialect).toBe("sqlite");
      expect(cfg.filePath).toBe("./data.db");
      expect(cfg.queryOptions?.["table"]).toBe("users");
    });

    it("should parse PostgreSQL config without exposing credentials", () => {
      const cfg = parseDatabaseUrl("postgres://myuser:secret@localhost:5433/mydb");
      expect(cfg.dialect).toBe("postgres");
      expect(cfg.user).toBe("myuser");
      expect(cfg.password).toBe("secret");
      expect(cfg.sanitizedUrl).toBe("postgres://myuser:***@localhost:5433/mydb");
    });
  });

  // 2. Type Mappings
  describe("Dialect Schema & Type Mappings", () => {
    it("should map logical types to PostgreSQL, MySQL, and SQLite data types", () => {
      expect(rowpipeTypeToSqlite("bigint")).toBe("INTEGER");
      expect(rowpipeTypeToPostgres("bigint")).toBe("BIGINT");
      expect(rowpipeTypeToMysql("bigint")).toBe("BIGINT");

      expect(rowpipeTypeToSqlite("decimal")).toBe("TEXT");
      expect(rowpipeTypeToPostgres("decimal")).toBe("NUMERIC");
      expect(rowpipeTypeToMysql("decimal")).toBe("DECIMAL(65,30)");

      expect(rowpipeTypeToSqlite("json")).toBe("TEXT");
      expect(rowpipeTypeToPostgres("json")).toBe("JSONB");
      expect(rowpipeTypeToMysql("json")).toBe("JSON");

      expect(rowpipeTypeToSqlite("binary")).toBe("BLOB");
      expect(rowpipeTypeToPostgres("binary")).toBe("BYTEA");
      expect(rowpipeTypeToMysql("binary")).toBe("LONGBLOB");
    });

    it("should generate clean CREATE TABLE DDL from inferred schema", () => {
      const schema: Schema = {
        columns: [
          { name: "id", type: "integer", nullable: false },
          { name: "name", type: "string", nullable: true },
          { name: "balance", type: "decimal", nullable: true },
          { name: "meta", type: "json", nullable: true },
          { name: "created_at", type: "datetime", nullable: false },
        ],
      };

      const pgSql = generateCreateTableSql("users", schema, "postgres");
      expect(pgSql).toContain('CREATE TABLE IF NOT EXISTS "users"');
      expect(pgSql).toContain('"id" INTEGER NOT NULL');
      expect(pgSql).toContain('"name" TEXT');
      expect(pgSql).toContain('"balance" NUMERIC');
      expect(pgSql).toContain('"meta" JSONB');
      expect(pgSql).toContain('"created_at" TIMESTAMPTZ NOT NULL');

      const sqliteSql = generateCreateTableSql("users", schema, "sqlite");
      expect(sqliteSql).toContain('CREATE TABLE IF NOT EXISTS "users"');
      expect(sqliteSql).toContain('"balance" TEXT');
    });
  });

  // 3. SQLite Streaming Reader
  describe("SQLite Cursor & Streaming Reader", () => {
    it("should stream database rows in bounded batches", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${DB_FILE}`, filePath: DB_FILE, sanitizedUrl: `sqlite://${DB_FILE}` });
      await adapter.executeRaw("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, val REAL)");

      // Insert 250 rows
      const insertRows = Array.from({ length: 250 }, (_, i) => ({
        id: i + 1,
        name: `Item_${i + 1}`,
        val: (i + 1) * 1.5,
      }));
      await adapter.insertBatch("items", insertRows);

      // Stream with batch size 60
      const reader = new DatabaseReader(adapter, { table: "items", batchSize: 60 });
      const batches: DataBatch[] = [];
      for await (const batch of reader.read()) {
        batches.push(batch);
      }

      expect(batches.length).toBe(5); // 60 + 60 + 60 + 60 + 10 = 250
      expect(batches[0].rows.length).toBe(60);
      expect(batches[4].rows.length).toBe(10);
      expect(batches[0].rows[0].name).toBe("Item_1");
      expect(batches[4].rows[9].name).toBe("Item_250");

      await reader.close();
    });

    it("should preserve BIGINT, Decimals, Dates, JSON, and NULLs without precision loss", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${DB_FILE}`, filePath: DB_FILE, sanitizedUrl: `sqlite://${DB_FILE}` });
      await adapter.executeRaw(`
        CREATE TABLE complex_types (
          id INTEGER PRIMARY KEY,
          big_num INTEGER,
          dec_num TEXT,
          json_data TEXT,
          created_at TEXT,
          nullable_col TEXT
        )
      `);

      const maxSafeBigInt = 9223372036854775807n;
      const testRows: Row[] = [
        {
          id: 1,
          big_num: maxSafeBigInt,
          dec_num: "123456789.123456789123456789",
          json_data: JSON.stringify({ role: "admin", tags: ["a", "b"] }),
          created_at: "2026-09-16T10:00:00.000Z",
          nullable_col: null,
        },
      ];

      await adapter.insertBatch("complex_types", testRows);

      const reader = new DatabaseReader(adapter, { table: "complex_types" });
      const stream = reader.read();
      let firstRow: Row | null = null;
      for await (const batch of stream) {
        firstRow = batch.rows[0];
      }

      expect(firstRow).toBeDefined();
      expect(firstRow?.id).toBe(1n);
      expect(firstRow?.big_num).toBe(maxSafeBigInt);
      expect(firstRow?.dec_num).toBe("123456789.123456789123456789");
      expect(firstRow?.nullable_col).toBeNull();

      await reader.close();
    });

    it("should respect AbortSignal and terminate streaming immediately", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${DB_FILE}`, filePath: DB_FILE, sanitizedUrl: `sqlite://${DB_FILE}` });
      await adapter.executeRaw("CREATE TABLE large_data (id INTEGER PRIMARY KEY, val TEXT)");
      const rows = Array.from({ length: 1000 }, (_, i) => ({ id: i + 1, val: `Row ${i + 1}` }));
      await adapter.insertBatch("large_data", rows);

      const abortController = new AbortController();
      const reader = new DatabaseReader(adapter, { table: "large_data", batchSize: 50, signal: abortController.signal });

      let batchesRead = 0;
      for await (const _batch of reader.read()) {
        batchesRead++;
        if (batchesRead === 2) {
          abortController.abort();
        }
      }

      // Should have stopped after 2 batches instead of reading all 20 batches
      expect(batchesRead).toBe(2);
      await reader.close();
    });
  });

  // 4. Database Writer (Sink)
  describe("Database Writer (Sink) & DDL / Upsert / Transactions", () => {
    it("should write DataBatches into database table with automatic table creation", async () => {
      const writer = new DatabaseWriter(`sqlite://${TARGET_DB_FILE}`, {
        table: "imported_users",
        createTable: true,
        batchSize: 50,
      });

      async function* generateBatches(): AsyncIterable<DataBatch> {
        yield {
          rows: [
            { id: 1, name: "Alice", email: "alice@example.com", age: 30 },
            { id: 2, name: "Bob", email: "bob@example.com", age: 25 },
          ],
          offset: 0,
          schema: {
            columns: [
              { name: "id", type: "integer", nullable: false },
              { name: "name", type: "string", nullable: true },
              { name: "email", type: "string", nullable: true },
              { name: "age", type: "integer", nullable: true },
            ],
          },
        };
        yield {
          rows: [
            { id: 3, name: "Charlie", email: "charlie@example.com", age: 35 },
          ],
          offset: 2,
        };
      }

      await writer.write(generateBatches());
      expect(writer.getLastResult()?.rowsWritten).toBe(3);
      await writer.close();

      // Verify table content in target database
      const verifyAdapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${TARGET_DB_FILE}`, filePath: TARGET_DB_FILE, sanitizedUrl: `sqlite://${TARGET_DB_FILE}` });
      const schema = await verifyAdapter.getTableSchema("imported_users");
      expect(schema.columns.map((c) => c.name)).toEqual(["id", "name", "email", "age"]);

      const rows: Row[] = [];
      for await (const b of verifyAdapter.stream({ table: "imported_users" })) {
        rows.push(...b.rows);
      }
      expect(rows.length).toBe(3);
      expect(rows[0].name).toBe("Alice");
      expect(rows[2].name).toBe("Charlie");
      await verifyAdapter.close();
    });

    it("should support upsert on conflict columns", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${TARGET_DB_FILE}`, filePath: TARGET_DB_FILE, sanitizedUrl: `sqlite://${TARGET_DB_FILE}` });
      await adapter.executeRaw("CREATE TABLE products (id INTEGER PRIMARY KEY, title TEXT, price REAL)");
      await adapter.insertBatch("products", [
        { id: 101, title: "Book", price: 19.99 },
        { id: 102, title: "Pen", price: 2.5 },
      ]);
      await adapter.close();

      // Write with upsert
      const writer = new DatabaseWriter(`sqlite://${TARGET_DB_FILE}`, {
        table: "products",
        upsert: true,
        conflictColumns: ["id"],
      });

      async function* upsertBatches(): AsyncIterable<DataBatch> {
        yield {
          rows: [
            { id: 101, title: "Updated Book", price: 24.99 },
            { id: 103, title: "Notebook", price: 5.0 },
          ],
          offset: 0,
        };
      }

      await writer.write(upsertBatches());
      await writer.close();

      const verifyAdapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${TARGET_DB_FILE}`, filePath: TARGET_DB_FILE, sanitizedUrl: `sqlite://${TARGET_DB_FILE}` });
      const rows: Row[] = [];
      for await (const b of verifyAdapter.stream({ table: "products" })) {
        rows.push(...b.rows);
      }

      expect(rows.length).toBe(3);
      const item101 = rows.find((r) => Number(r.id) === 101);
      expect(item101?.title).toBe("Updated Book");
      expect(item101?.price).toBe(24.99);

      const item103 = rows.find((r) => Number(r.id) === 103);
      expect(item103?.title).toBe("Notebook");
      await verifyAdapter.close();
    });
  });

  // 5. Pushdown Optimizer
  describe("Pushdown Optimizer Analysis", () => {
    it("should push down SELECT, WHERE, ORDER BY, LIMIT, and OFFSET into SQL query", () => {
      const analysis = analyzeDatabasePushdown(
        "users",
        "postgres",
        [
          { type: "filter", expression: "age > 30 and active == true" },
          { type: "select", columns: ["id", "name", "age"] },
          { type: "sort", specs: [{ column: "age", direction: "desc" }] },
          { type: "offset", count: 20 },
          { type: "limit", count: 100 },
        ]
      );

      expect(analysis.generatedQuery).toBe(
        'SELECT "id", "name", "age" FROM "users" WHERE ("age" > 30 AND "active" = TRUE) ORDER BY "age" DESC LIMIT 100 OFFSET 20'
      );
      expect(analysis.remainingOperations.length).toBe(0); // Fully pushed down!
    });

    it("should keep unsupported filter expressions in local pipeline while pushing down safe operations", () => {
      const analysis = analyzeDatabasePushdown(
        "events",
        "sqlite",
        [
          { type: "filter", expression: "custom_fn(data) > 10" }, // Unsupported custom function
          { type: "select", columns: ["id", "timestamp"] },
          { type: "limit", count: 50 },
        ]
      );

      expect(analysis.generatedQuery).toBe(
        'SELECT "id", "timestamp" FROM "events" LIMIT 50'
      );
      // Filter must remain local
      expect(analysis.remainingOperations.length).toBe(1);
      expect(analysis.remainingOperations[0].type).toBe("filter");
    });
  });

  // 6. CLI End-to-End Integration
  describe("CLI Database Commands", () => {
    it("should inspect tables and schemas with rowpipe db --tables and --schema", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${DB_FILE}`, filePath: DB_FILE, sanitizedUrl: `sqlite://${DB_FILE}` });
      await adapter.executeRaw(`
        CREATE TABLE customers (
          id INTEGER PRIMARY KEY,
          email TEXT NOT NULL,
          created_at TEXT
        );
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          customer_id INTEGER,
          total REAL
        );
      `);
      await adapter.close();

      // rowpipe db <url> --tables --json
      const tablesOutput = execSync(`node ${CLI_PATH} db sqlite://${DB_FILE} --tables --json`).toString();
      const tables = JSON.parse(tablesOutput);
      expect(tables).toContain("customers");
      expect(tables).toContain("orders");

      // rowpipe db <url> --schema customers --json
      const schemaOutput = execSync(`node ${CLI_PATH} db sqlite://${DB_FILE} --schema customers --json`).toString();
      const schema = JSON.parse(schemaOutput);
      expect(schema.table).toBe("customers");
      expect(schema.columns.map((c: any) => c.name)).toEqual(["id", "email", "created_at"]);
    });

    it("should stream database table directly to JSONL and CSV", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${DB_FILE}`, filePath: DB_FILE, sanitizedUrl: `sqlite://${DB_FILE}` });
      await adapter.executeRaw("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
      await adapter.insertBatch("users", [
        { id: 1, name: "Alice", age: 30 },
        { id: 2, name: "Bob", age: 25 },
        { id: 3, name: "Charlie", age: 35 },
      ]);
      await adapter.close();

      const jsonlOut = execSync(`node ${CLI_PATH} db sqlite://${DB_FILE} --table users --to jsonl`).toString();
      const lines = jsonlOut.trim().split("\n");
      expect(lines.length).toBe(3);
      expect(JSON.parse(lines[0]).name).toBe("Alice");

      const csvOut = execSync(`node ${CLI_PATH} db sqlite://${DB_FILE} --table users --filter 'age > 26' --to csv`).toString();
      expect(csvOut).toContain("Alice");
      expect(csvOut).toContain("Charlie");
      expect(csvOut).not.toContain("Bob");
    });

    it("should import CSV file into SQLite database via --to-db", async () => {
      const csvPath = join(TEST_DIR, "import.csv");
      writeFileSync(csvPath, "id,city,population\n1,Tokyo,37400000\n2,Delhi,29300000\n3,Shanghai,26300000\n");

      execSync(`"${process.execPath}" --experimental-sqlite "${CLI_PATH}" "${csvPath}" --to-db sqlite://${TARGET_DB_FILE} --to-table cities --create-table`);

      const verifyAdapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${TARGET_DB_FILE}`, filePath: TARGET_DB_FILE, sanitizedUrl: `sqlite://${TARGET_DB_FILE}` });
      const schema = await verifyAdapter.getTableSchema("cities");
      expect(schema).toBeDefined();

      const output = execSync(`"${process.execPath}" --experimental-sqlite "${CLI_PATH}" db sqlite://${TARGET_DB_FILE} --table cities --to jsonl`).toString();
      const lines = output.trim().split("\n");
      expect(lines.length).toBe(3);
      expect(JSON.parse(lines[0]).city).toBe("Tokyo");
      await verifyAdapter.close();
    });

    it("should stream migrate between two databases (db -> db)", async () => {
      const sourceDb = join(TEST_DIR, "source.db");
      const destDb = join(TEST_DIR, "dest.db");

      const srcAdapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${sourceDb}`, filePath: sourceDb, sanitizedUrl: `sqlite://${sourceDb}` });
      await srcAdapter.executeRaw("CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, message TEXT)");
      await srcAdapter.insertBatch("logs", [
        { id: 1, level: "INFO", message: "System start" },
        { id: 2, level: "ERROR", message: "Null pointer" },
        { id: 3, level: "INFO", message: "Health check" },
      ]);
      await srcAdapter.close();

      // db -> db with filter and create table
      execSync(`"${process.execPath}" --experimental-sqlite "${CLI_PATH}" db sqlite://${sourceDb} --table logs --filter 'level == "ERROR"' --to-db sqlite://${destDb} --to-table error_logs --create-table`);

      const destAdapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${destDb}`, filePath: destDb, sanitizedUrl: `sqlite://${destDb}` });
      const destOut = execSync(`"${process.execPath}" --experimental-sqlite "${CLI_PATH}" db sqlite://${destDb} --table error_logs --to jsonl`).toString();
      const lines = destOut.trim().split("\n");
      expect(lines.length).toBe(1);
      expect(JSON.parse(lines[0]).message).toBe("Null pointer");
      await destAdapter.close();
    });

    it("should diff a database table against a CSV file", async () => {
      const adapter = new SqliteDatabaseAdapter({ dialect: "sqlite", url: `sqlite://${DB_FILE}`, filePath: DB_FILE, sanitizedUrl: `sqlite://${DB_FILE}` });
      await adapter.executeRaw("CREATE TABLE members (id INTEGER PRIMARY KEY, name TEXT, role TEXT)");
      await adapter.insertBatch("members", [
        { id: 1, name: "Alice", role: "admin" },
        { id: 2, name: "Bob", role: "user" },
      ]);
      await adapter.close();

      const csvPath = join(TEST_DIR, "members.csv");
      writeFileSync(csvPath, "id,name,role\n1,Alice,superadmin\n2,Bob,user\n3,Charlie,member\n");

      // diff db table against CSV file with --coerce
      const diffOut = execSync(
        `"${process.execPath}" --experimental-sqlite "${CLI_PATH}" diff 'sqlite://${DB_FILE}?table=members' "${csvPath}" --key id --coerce --json`
      ).toString();

      const diff = JSON.parse(diffOut);
      expect(diff.rows.added).toBe(1); // Charlie
      expect(diff.rows.changed).toBe(1); // Alice role changed admin -> superadmin
      expect(diff.rows.unchanged).toBe(1); // Bob
    });

    it("should explain execution plan with database pushdown and secret masking", () => {
      const explainOut = execSync(
        `node ${CLI_PATH} explain db postgres://admin:secretPass@localhost:5432/app --table users --filter 'age > 30' --select id,name,age --limit 100`
      ).toString();

      expect(explainOut).toContain("Remote Database Pushdown:");
      expect(explainOut).toContain('SELECT "id", "name", "age" FROM "users" WHERE "age" > 30 LIMIT 100');
      expect(explainOut).toContain("postgres://admin:***@localhost:5432/app");
      expect(explainOut).not.toContain("secretPass");
    });
  });
});
