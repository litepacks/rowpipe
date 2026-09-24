import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { promises as fs } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import writeXlsxFile from "write-excel-file/node";

const execAsync = promisify(exec);
const cliPath = join(process.cwd(), "dist/cli/index.js");

describe("CLI Integration Tests", { timeout: 30000 }, () => {
  const tempDir = join(tmpdir(), `rowpipe_cli_test_${Date.now()}`);
  const sampleCsv = join(tempDir, "sample.csv");
  const sampleSchema = join(tempDir, "sample.schema.json");
  const sampleXlsx = join(tempDir, "workbook.xlsx");

  beforeAll(async () => {
    await fs.mkdir(tempDir, { recursive: true });

    // Create test CSV
    const csvContent = `id,name,age,email,revenue\n1,Alice,30,alice@test.com,150.50\n2,Bob,17,bob@test.com,50.00\n3,Charlie,45,charlie@test.com,500.25\n4,David,15,david@test.com,10.00\n`;
    await fs.writeFile(sampleCsv, csvContent, "utf8");

    // Create test schema
    const schemaContent = JSON.stringify({
      id: { type: "integer", nullable: false },
      name: { type: "string", nullable: false },
      age: { type: "integer", nullable: false },
      email: { type: "string", nullable: false, format: "email" },
      revenue: { type: "number", nullable: false },
    });
    await fs.writeFile(sampleSchema, schemaContent, "utf8");

    // Create multi-sheet XLSX workbook
    const usersSheet = [
      [{ value: "id", fontWeight: "bold" }, { value: "name", fontWeight: "bold" }, { value: "email", fontWeight: "bold" }],
      [{ value: 1 }, { value: "Alice" }, { value: "alice@example.com" }],
      [{ value: 2 }, { value: "Bob" }, { value: "bob@example.com" }],
    ];
    const ordersSheet = [
      [{ value: "order_id", fontWeight: "bold" }, { value: "user_id", fontWeight: "bold" }, { value: "total", fontWeight: "bold" }],
      [{ value: "ORD-101" }, { value: 1 }, { value: 99.9 }],
      [{ value: "ORD-102" }, { value: 2 }, { value: 149.5 }],
      [{ value: "ORD-103" }, { value: 1 }, { value: 25.0 }],
    ];

    await (writeXlsxFile as unknown as (sheets: unknown[], opts?: unknown) => { toFile: (path: string) => Promise<void> })(
      [
        { data: usersSheet, sheet: "Users" },
        { data: ordersSheet, sheet: "Orders" },
      ]
    ).toFile(sampleXlsx);
  });

  afterAll(async () => {
    await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  });

  it("should inspect CSV file", async () => {
    const { stdout } = await execAsync(`node ${cliPath} inspect ${sampleCsv}`);
    expect(stdout).toContain("Rows: 4");
    expect(stdout).toContain("Columns: 5");
    expect(stdout).toContain("email");
  });

  it("should inspect with --json output", async () => {
    const { stdout } = await execAsync(`node ${cliPath} inspect ${sampleCsv} --json`);
    const json = JSON.parse(stdout);
    expect(json.rows).toBe(4);
    expect(json.columnsCount).toBe(5);
  });

  it("should convert CSV to JSONL", async () => {
    const outputJsonl = join(tempDir, "output.jsonl");
    await execAsync(`node ${cliPath} convert ${sampleCsv} ${outputJsonl}`);

    const content = await fs.readFile(outputJsonl, "utf8");
    const lines = content.trim().split("\n");
    expect(lines.length).toBe(4);
    expect(JSON.parse(lines[0]!).name).toBe("Alice");
  });

  it("should infer schema", async () => {
    const { stdout } = await execAsync(`node ${cliPath} schema ${sampleCsv}`);
    expect(stdout).toContain("COLUMN");
    expect(stdout).toContain("id");
    expect(stdout).toContain("integer");
    expect(stdout).toContain("email");
  });

  it("should calculate statistics", async () => {
    const { stdout } = await execAsync(`node ${cliPath} stats ${sampleCsv}`);
    expect(stdout).toContain("Total Rows: 4");
    expect(stdout).toContain("revenue (numeric)");
    expect(stdout).toContain("mean");
  });

  it("should filter stream", async () => {
    const { stdout } = await execAsync(`node ${cliPath} filter ${sampleCsv} "age >= 18"`);
    const lines = stdout.trim().split("\n");
    expect(lines.length).toBe(3);
    expect(stdout).toContain("Alice");
    expect(stdout).toContain("Charlie");
    expect(stdout).not.toContain("David");
  });

  it("should select columns", async () => {
    const { stdout } = await execAsync(`node ${cliPath} select ${sampleCsv} id,email`);
    const lines = stdout.trim().split("\n");
    expect(lines[0]).toBe("id,email");
    expect(lines[1]).toBe("1,alice@test.com");
  });

  it("should execute Unix pipe chain", async () => {
    const cmd = `cat ${sampleCsv} | node ${cliPath} filter - "age >= 18" | node ${cliPath} select - id,name,age | node ${cliPath} convert - --from csv --to jsonl`;
    const { stdout } = await execAsync(cmd);
    const lines = stdout.trim().split("\n");
    expect(lines.length).toBe(2);

    const row1 = JSON.parse(lines[0]!);
    expect(row1).toEqual({ id: "1", name: "Alice", age: "30" });
    const row2 = JSON.parse(lines[1]!);
    expect(row2).toEqual({ id: "3", name: "Charlie", age: "45" });
  });

  it("should validate CSV against schema", async () => {
    const { stdout } = await execAsync(`node ${cliPath} validate ${sampleCsv} --schema ${sampleSchema}`);
    expect(stdout).toContain("4 rows scanned");
    expect(stdout).toContain("4 valid");
    expect(stdout).toContain("0 invalid");
  });

  it("should inspect multi-sheet XLSX workbook", async () => {
    const { stdout } = await execAsync(`node ${cliPath} inspect ${sampleXlsx}`);
    expect(stdout).toContain("Sheets: 2");
    expect(stdout).toContain("Users");
    expect(stdout).toContain("Orders");
  });

  it("should inspect specific XLSX sheet", async () => {
    const { stdout } = await execAsync(`node ${cliPath} inspect ${sampleXlsx} --sheet Orders`);
    expect(stdout).toContain("order_id");
    expect(stdout).toContain("total");
    expect(stdout).toContain("Rows: 3");
  });

  it("should convert specific XLSX sheet to CSV", async () => {
    const ordersCsv = join(tempDir, "orders.csv");
    await execAsync(`node ${cliPath} convert ${sampleXlsx} ${ordersCsv} --sheet Orders`);
    const content = await fs.readFile(ordersCsv, "utf8");
    expect(content).toContain("order_id,user_id,total");
    expect(content).toContain("ORD-101");
  });

  it("should export all sheets from workbook using --all-sheets", async () => {
    const exportDir = join(tempDir, "all_exported");
    await execAsync(`node ${cliPath} convert ${sampleXlsx} --all-sheets --out-dir ${exportDir} --to csv`);

    const usersContent = await fs.readFile(join(exportDir, "Users.csv"), "utf8");
    expect(usersContent).toContain("Alice");

    const ordersContent = await fs.readFile(join(exportDir, "Orders.csv"), "utf8");
    expect(ordersContent).toContain("ORD-101");
  });

  it("should execute rowpipe map to derive new columns", async () => {
    const { stdout } = await execAsync(
      `node ${cliPath} map ${sampleCsv} "is_adult=age >= 30" "tax=revenue * 0.20"`
    );
    expect(stdout).toContain("is_adult");
    expect(stdout).toContain("tax");
    expect(stdout).toContain("30.1");
  });

  it("should execute rowpipe reduce to aggregate stream", async () => {
    const { stdout } = await execAsync(
      `node ${cliPath} reduce ${sampleCsv} "total_rev=sum(revenue)" "avg_age=avg(age)" "total=count()"`
    );
    expect(stdout).toContain("total_rev,avg_age,total");
    expect(stdout).toContain("710.75,26.75,4");
  });

  it("should stream files as CSV and filter through Unix pipes", async () => {
    const { stdout } = await execAsync(
      `node ${cliPath} files ${tempDir} --include "*.csv" | node ${cliPath} filter - 'size > 0' | node ${cliPath} select - relative_path,extension,type`
    );
    expect(stdout).toContain("relative_path,extension,type");
    expect(stdout).toContain("sample.csv,csv,file");
  });

  it("should generate deterministic file snapshots with hashes", async () => {
    const { stdout } = await execAsync(
      `node ${cliPath} files snapshot ${tempDir} --include "*.csv" --hash sha256 --to jsonl`
    );
    expect(stdout).toContain('"relative_path":"sample.csv"');
    expect(stdout).toContain('"hash":');
  });

  it("should diff two directories using rowpipe files diff", async () => {
    const subA = join(tempDir, "subA");
    const subB = join(tempDir, "subB");
    await fs.mkdir(subA, { recursive: true });
    await fs.mkdir(subB, { recursive: true });
    await fs.writeFile(join(subA, "test.txt"), "hello v1");
    await fs.writeFile(join(subB, "test.txt"), "hello v2");

    const { stdout } = await execAsync(
      `node ${cliPath} files diff ${subA} ${subB} --json`
    );
    const summary = JSON.parse(stdout);
    expect(summary.rows.changed).toBe(1);
  });

  it("should return correct version with --version, -v, -V, and version command", async () => {
    const pkg = JSON.parse(await fs.readFile(join(process.cwd(), "package.json"), "utf8"));
    const expectedVersion = pkg.version;

    const res1 = await execAsync(`node ${cliPath} --version`);
    expect(res1.stdout.trim()).toBe(expectedVersion);

    const res2 = await execAsync(`node ${cliPath} -v`);
    expect(res2.stdout.trim()).toBe(expectedVersion);

    const res3 = await execAsync(`node ${cliPath} -V`);
    expect(res3.stdout.trim()).toBe(expectedVersion);

    const res4 = await execAsync(`node ${cliPath} version`);
    expect(res4.stdout.trim()).toBe(expectedVersion);
  });

  it("should handle empty parameter without hanging or crashing", async () => {
    const resRoot = await execAsync(`node ${cliPath} ""`);
    expect(resRoot.stdout).toContain("Usage: rowpipe");

    const resInspect = await execAsync(`node ${cliPath} inspect ""`);
    expect(resInspect.stdout).toContain("Usage: rowpipe inspect");
  });
});


