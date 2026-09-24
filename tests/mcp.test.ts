import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { writeFile, unlink, mkdir, rm } from "node:fs/promises";
import { join } from "node:path";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createRowpipeMcpServer } from "../src/mcp/index.js";
import { VERSION } from "../src/version.js";

const execFileAsync = promisify(execFile);
const TEST_DIR = join(process.cwd(), "scratch_test_mcp");
const SAMPLE_CSV = join(TEST_DIR, "users.csv");
const MODIFIED_CSV = join(TEST_DIR, "users_modified.csv");
const CONVERT_OUT_JSON = join(TEST_DIR, "users.json");

describe("Rowpipe Model Context Protocol (MCP) Server Suite", () => {
  const sampleCsvData = [
    "id,name,role,age,salary",
    "1,Alice,Engineer,28,120000",
    "2,Bob,Designer,34,95000",
    "3,Charlie,Product,42,135000",
    "4,Diana,Engineer,31,125000",
    "5,Eve,Engineering Lead,39,150000",
  ].join("\n");

  const modifiedCsvData = [
    "id,name,role,age,salary",
    "1,Alice,Senior Engineer,29,130000", // changed role, age, salary
    "2,Bob,Designer,34,95000", // unchanged
    "4,Diana,Engineer,31,125000", // unchanged (Charlie id:3 removed)
    "6,Frank,Data Scientist,27,110000", // added id:6
  ].join("\n");

  beforeAll(async () => {
    await mkdir(TEST_DIR, { recursive: true });
    await writeFile(SAMPLE_CSV, sampleCsvData, "utf8");
    await writeFile(MODIFIED_CSV, modifiedCsvData, "utf8");
  });

  afterAll(async () => {
    await rm(TEST_DIR, { recursive: true, force: true });
  });

  describe("Server Initialization & Registry", () => {
    it("should instantiate rowpipe MCP server with 9 registered tools and resources", () => {
      const app = createRowpipeMcpServer({ registerInCentral: false });
      const tools = app.getTools();

      expect(tools.length).toBe(9);
      const toolNames = tools.map((t) => t.name).sort();
      expect(toolNames).toEqual([
        "rowpipe_convert",
        "rowpipe_diff",
        "rowpipe_inspect",
        "rowpipe_profile",
        "rowpipe_query",
        "rowpipe_sample",
        "rowpipe_schema",
        "rowpipe_stats",
        "rowpipe_table",
      ]);

      const templates = app.getResourceTemplates();
      expect(templates.some((t) => t.uriTemplate === "rowpipe://metadata/{filePath}")).toBe(true);
      expect(templates.some((t) => t.uriTemplate === "rowpipe://schema/{filePath}")).toBe(true);
    });
  });

  describe("Tool Execution (Programmatic)", () => {
    const app = createRowpipeMcpServer({ registerInCentral: false });

    it("should execute rowpipe_inspect on CSV dataset", async () => {
      const res = await app.callTool("rowpipe_inspect", { filePath: SAMPLE_CSV });
      expect(res.isError).toBeFalsy();
      expect(res.data.rows).toBe(5);
      expect(res.data.columnsCount).toBe(5);
      expect(res.data.format).toBe("CSV");
      expect(res.data.columns.some((c: any) => c.name === "salary" && c.type === "integer")).toBe(true);
    });

    it("should execute rowpipe_query with filter, select, and sort", async () => {
      const res = await app.callTool("rowpipe_query", {
        filePath: SAMPLE_CSV,
        filter: "age >= 30",
        select: "name,role,salary",
        sort: "salary:desc",
        limit: 10,
      });

      expect(res.isError).toBeFalsy();
      expect(res.data.totalReturned).toBe(4);
      expect(res.data.rows[0].name).toBe("Eve"); // highest salary 150000
      expect(res.data.rows[0].salary).toBe("150000");
    });

    it("should format rowpipe_query as markdown table", async () => {
      const res = await app.callTool("rowpipe_query", {
        filePath: SAMPLE_CSV,
        select: "id,name",
        limit: 2,
        format: "markdown",
      });

      expect(res.isError).toBeFalsy();
      expect(res.text).toContain("| id | name |");
      expect(res.text).toContain("| 1 | Alice |");
      expect(res.text).toContain("| 2 | Bob |");
    });

    it("should execute rowpipe_schema inference", async () => {
      const res = await app.callTool("rowpipe_schema", { filePath: SAMPLE_CSV });
      expect(res.isError).toBeFalsy();
      expect(res.data.totalRowsScanned).toBe(5);
      const colMap = new Map(res.data.columns.map((c: any) => [c.name, c.type]));
      expect(colMap.get("id")).toBe("integer");
      expect(colMap.get("name")).toBe("string");
      expect(colMap.get("age")).toBe("integer");
      expect(colMap.get("salary")).toBe("integer");
    });

    it("should execute rowpipe_stats for numerical columns", async () => {
      const res = await app.callTool("rowpipe_stats", { filePath: SAMPLE_CSV, column: "salary" });
      expect(res.isError).toBeFalsy();
      expect(res.data.totalRows).toBe(5);
      const salaryStats = res.data.columns.salary?.numeric;
      expect(salaryStats).toBeDefined();
      expect(salaryStats.min).toBe(95000);
      expect(salaryStats.max).toBe(150000);
      expect(salaryStats.count).toBe(5);
    });

    it("should execute rowpipe_sample reservoir sampling", async () => {
      const res = await app.callTool("rowpipe_sample", {
        filePath: SAMPLE_CSV,
        size: 3,
        seed: 12345,
      });
      expect(res.isError).toBeFalsy();
      expect(res.data.sampleSize).toBe(3);
      expect(res.data.rows.length).toBe(3);
    });

    it("should execute rowpipe_convert from CSV to JSON", async () => {
      const res = await app.callTool("rowpipe_convert", {
        inputPath: SAMPLE_CSV,
        outputPath: CONVERT_OUT_JSON,
      });
      expect(res.isError).toBeFalsy();
      expect(res.data.success).toBe(true);
      expect(res.data.outputSizeBytes).toBeGreaterThan(0);
    });

    it("should execute rowpipe_diff comparing two datasets", async () => {
      const res = await app.callTool("rowpipe_diff", {
        leftPath: SAMPLE_CSV,
        rightPath: MODIFIED_CSV,
        key: "id",
      });

      expect(res.isError).toBeFalsy();
      expect(res.data.rows.added).toBe(1); // Frank (6)
      expect(res.data.rows.removed).toBe(2); // Charlie (3), Eve (5)
      expect(res.data.rows.changed).toBe(1); // Alice (1)
      expect(res.data.rows.unchanged).toBe(2); // Bob (2), Diana (4)
    });

    it("should execute rowpipe_profile", async () => {
      const res = await app.callTool("rowpipe_profile", {
        filePath: SAMPLE_CSV,
        format: "json",
      });
      expect(res.isError).toBeFalsy();
      expect(res.data.totalRows).toBe(5);
      expect(res.data.totalColumns).toBe(5);
    });

    it("should execute rowpipe_table preview", async () => {
      const res = await app.callTool("rowpipe_table", {
        filePath: SAMPLE_CSV,
        limit: 2,
        select: "name,role",
      });
      expect(res.isError).toBeFalsy();
      expect(res.text).toContain("name");
      expect(res.text).toContain("role");
      expect(res.text).toContain("Alice");
      expect(res.text).toContain("Bob");
    });
  });

  describe("Dynamic Resource Templates", () => {
    const app = createRowpipeMcpServer({ registerInCentral: false });

    it("should read rowpipe://metadata resource", async () => {
      const uri = `rowpipe://metadata/${encodeURIComponent(SAMPLE_CSV)}`;
      const res = await app.readResource(uri);
      expect(res.contents.length).toBe(1);
      expect(res.contents[0].mimeType).toBe("application/json");
      const parsed = JSON.parse(res.contents[0].text);
      expect(parsed.rows).toBe(5);
      expect(parsed.format).toBe("CSV");
    });

    it("should read rowpipe://schema resource", async () => {
      const uri = `rowpipe://schema/${encodeURIComponent(SAMPLE_CSV)}`;
      const res = await app.readResource(uri);
      expect(res.contents.length).toBe(1);
      expect(res.contents[0].mimeType).toBe("application/json");
      const parsed = JSON.parse(res.contents[0].text);
      expect(parsed.totalRowsScanned).toBe(5);
    });
  });

  describe("CLI Command Integration", () => {
    const cliBin = join(process.cwd(), "dist", "cli", "index.js");

    it("should list all 9 tools via 'rowpipe mcp tools'", async () => {
      const { stdout } = await execFileAsync("node", [cliBin, "mcp", "tools"]);
      expect(stdout).toContain("Registered Tools (9):");
      expect(stdout).toContain("rowpipe_inspect");
      expect(stdout).toContain("rowpipe_query");
      expect(stdout).toContain("rowpipe_schema");
      expect(stdout).toContain("rowpipe_stats");
      expect(stdout).toContain("rowpipe_sample");
      expect(stdout).toContain("rowpipe_convert");
      expect(stdout).toContain("rowpipe_diff");
      expect(stdout).toContain("rowpipe_profile");
      expect(stdout).toContain("rowpipe_table");
    });

    it("should execute 'rowpipe mcp call rowpipe_inspect --filePath ...'", async () => {
      const { stdout } = await execFileAsync("node", [
        cliBin,
        "mcp",
        "call",
        "rowpipe_inspect",
        "--filePath",
        SAMPLE_CSV,
      ]);
      const res = JSON.parse(stdout);
      expect(res.rows).toBe(5);
      expect(res.columnsCount).toBe(5);
    });

    it("should show version via 'rowpipe mcp --version'", async () => {
      const { stdout } = await execFileAsync("node", [cliBin, "mcp", "--version"]);
      expect(stdout.trim()).toBe(VERSION);
    });
  });
});
