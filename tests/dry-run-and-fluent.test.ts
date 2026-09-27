import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdir, rm, writeFile, readFile, stat } from "node:fs/promises";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import rowpipeDefault, { rowpipe, Pipeline } from "../src/index.js";

const execAsync = promisify(exec);
const TEST_DIR = join(process.cwd(), "scratch_test_fluent_dryrun");
const CLI_PATH = join(process.cwd(), "dist/cli/index.js");

describe("Fluent Pipeline Builder API & --dry-run / --preview", () => {
  beforeEach(async () => {
    await rm(TEST_DIR, { recursive: true, force: true });
    await mkdir(TEST_DIR, { recursive: true });
  });

  afterEach(async () => {
    await rm(TEST_DIR, { recursive: true, force: true });
  });

  describe("Programmatic Fluent API (rowpipe.from)", () => {
    it("should export rowpipe builder both as default and named export", () => {
      expect(rowpipe).toBeDefined();
      expect(rowpipeDefault).toBeDefined();
      expect(typeof rowpipe.from).toBe("function");
      expect(typeof rowpipeDefault.from).toBe("function");
    });

    it("should chain fluent transforms on in-memory row collections", async () => {
      const inputData = [
        { id: 1, name: "Alice", age: 24, score: "88" },
        { id: 2, name: "Bob", age: 35, score: "92" },
        { id: 3, name: "Charlie", age: 41, score: "74" },
        { id: 4, name: "Diana", age: 29, score: "95" },
      ];

      const results = await rowpipe
        .from(inputData)
        .filter("age >= 30")
        .cast({ score: "integer" })
        .map("passed = score >= 80")
        .rename({ name: "fullName" })
        .select(["id", "fullName", "score", "passed"])
        .sort("score:desc")
        .toArray();

      expect(results).toEqual([
        { id: 2, fullName: "Bob", score: 92, passed: true },
        { id: 3, fullName: "Charlie", score: 74, passed: false },
      ]);
    });

    it("should count rows fluently without loading entire dataset into array", async () => {
      const rows = Array.from({ length: 50 }, (_, i) => ({ id: i + 1, val: i * 2 }));
      const total = await rowpipe
        .from(rows)
        .filter("val >= 20")
        .count();

      expect(total).toBe(40);
    });

    it("should convert file to file using string paths directly", async () => {
      const csvPath = join(TEST_DIR, "source.csv");
      const jsonlPath = join(TEST_DIR, "dest.jsonl");

      await writeFile(
        csvPath,
        "id,user,points\n1,Alpha,150\n2,Beta,250\n3,Gamma,350\n",
        "utf-8"
      );

      await rowpipe
        .from(csvPath)
        .filter("points > 200")
        .to(jsonlPath);

      const content = await readFile(jsonlPath, "utf-8");
      const lines = content.trim().split("\n").map((l) => JSON.parse(l));

      expect(lines).toHaveLength(2);
      expect(lines[0]).toEqual({ id: "2", user: "Beta", points: "250" });
      expect(lines[1]).toEqual({ id: "3", user: "Gamma", points: "350" });
    });

    it("should execute dryRun/preview programmatically without writing target file", async () => {
      const csvPath = join(TEST_DIR, "source.csv");
      const targetParquet = join(TEST_DIR, "should_not_exist.parquet");

      await writeFile(
        csvPath,
        "id,active,price,created_at\n1,true,19.99,2026-01-01\n2,false,49.50,2026-01-02\n3,true,9.00,2026-01-03\n",
        "utf-8"
      );

      const pipeline = rowpipe.from(csvPath);
      const preview = await pipeline.dryRun(2);

      expect(preview.totalSampled).toBe(2);
      expect(preview.columns).toEqual(["id", "active", "price", "created_at"]);
      expect(preview.types.id).toBe("integer");
      expect(preview.types.active).toBe("boolean");
      expect(preview.types.price).toBe("number");
      expect(preview.types.created_at).toBe("date");
      expect(preview.rows).toHaveLength(2);

      // Verify that using .to(..., { dryRun: true }) does NOT write to target
      await rowpipe.from(csvPath).to(targetParquet, { dryRun: true });
      await expect(stat(targetParquet)).rejects.toThrow();
    });
  });

  describe("CLI --dry-run and --preview on 'rowpipe convert'", () => {
    it("should execute 'rowpipe convert' in dry-run mode without creating target file", async () => {
      const csvPath = join(TEST_DIR, "input.csv");
      const outPath = join(TEST_DIR, "output.parquet");

      await writeFile(
        csvPath,
        "id,metric,value\n101,cpu,42.5\n102,memory,78.2\n103,disk,60.1\n",
        "utf-8"
      );

      const { stdout } = await execAsync(
        `node "${CLI_PATH}" convert "${csvPath}" "${outPath}" --dry-run`,
        { env: { ...process.env, FORCE_COLOR: "0" } }
      );

      expect(stdout).toContain("Pipeline Dry Run / Preview");
      expect(stdout).toContain("No files were written - dry-run mode active");
      expect(stdout).toContain("Inferred Column Types");

      // Verify target file was NEVER created
      await expect(stat(outPath)).rejects.toThrow();
    });

    it("should output machine-readable JSON in dry-run mode with --json", async () => {
      const csvPath = join(TEST_DIR, "input.csv");
      const outPath = join(TEST_DIR, "output.jsonl");

      await writeFile(
        csvPath,
        "name,age,admin\nAlice,30,true\nBob,25,false\nCharlie,40,true\n",
        "utf-8"
      );

      const { stdout } = await execAsync(
        `node "${CLI_PATH}" convert "${csvPath}" "${outPath}" --preview 2 --json`,
        { env: { ...process.env, FORCE_COLOR: "0" } }
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.dryRun).toBe(true);
      expect(parsed.source.format).toBe("csv");
      expect(parsed.target.format).toBe("jsonl");
      expect(parsed.previewCount).toBe(2);
      expect(parsed.columns).toEqual(["name", "age", "admin"]);
      expect(parsed.types.name).toBe("string");
      expect(parsed.types.age).toBe("integer");
      expect(parsed.types.admin).toBe("boolean");
      expect(parsed.rows).toHaveLength(2);

      await expect(stat(outPath)).rejects.toThrow();
    });
  });

  describe("CLI --dry-run and --preview on root pipeline", () => {
    it("should preview root pipeline execution without writing to target file", async () => {
      const csvPath = join(TEST_DIR, "pipeline_input.csv");
      const outCsv = join(TEST_DIR, "pipeline_out.csv");

      await writeFile(
        csvPath,
        "id,city,population\n1,Tokyo,37000000\n2,Delhi,30000000\n3,Shanghai,27000000\n",
        "utf-8"
      );

      const { stdout } = await execAsync(
        `node "${CLI_PATH}" "${csvPath}" --filter "population > 28000000" --select "city,population" --output "${outCsv}" --dry-run`,
        { env: { ...process.env, FORCE_COLOR: "0" } }
      );

      expect(stdout).toContain("Pipeline Dry Run / Preview");
      expect(stdout).toContain("filter -> select");
      expect(stdout).toContain("No data was written - dry-run mode active");

      // Verify target file was NOT created
      await expect(stat(outCsv)).rejects.toThrow();
    });

    it("should output machine-readable JSON for root pipeline with --dry-run --json", async () => {
      const csvPath = join(TEST_DIR, "pipeline_input.csv");

      await writeFile(
        csvPath,
        "id,code,active\n1,XYZ,true\n2,ABC,false\n",
        "utf-8"
      );

      const { stdout } = await execAsync(
        `node "${CLI_PATH}" "${csvPath}" --select "code,active" --preview 1 --json`,
        { env: { ...process.env, FORCE_COLOR: "0" } }
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.dryRun).toBe(true);
      expect(parsed.previewCount).toBe(1);
      expect(parsed.plan.operations).toContain("select");
      expect(parsed.rows).toHaveLength(1);
      expect(parsed.rows[0]).toEqual({ code: "XYZ", active: "true" });
    });
  });
});
