import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { MultiFileReader, resolveFilePatterns, isGlobPattern } from "../src/readers/multi-file.js";
import { createPipeline } from "../src/core/pipeline.js";
import { createReader } from "../src/readers/index.js";

describe("Multi-File & Glob Reader Test Suite", () => {
  let tmpDir: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "rowpipe_multi_test_"));
    fs.mkdirSync(path.join(tmpDir, "logs"), { recursive: true });

    // Create partitioned CSV files
    fs.writeFileSync(
      path.join(tmpDir, "logs", "part_1.csv"),
      "id,name,value\n1,Alice,100\n2,Bob,200\n"
    );
    fs.writeFileSync(
      path.join(tmpDir, "logs", "part_2.csv"),
      "id,name,value\n3,Charlie,300\n4,David,400\n"
    );
    fs.writeFileSync(
      path.join(tmpDir, "logs", "part_3.csv"),
      "id,name,value\n5,Eve,500\n"
    );
  });

  afterEach(() => {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore
    }
  });

  it("should recognize glob patterns", () => {
    expect(isGlobPattern("logs/*.csv")).toBe(true);
    expect(isGlobPattern("logs/data_{1,2}.json")).toBe(true);
    expect(isGlobPattern("logs/data_?.parquet")).toBe(true);
    expect(isGlobPattern("logs/part_1.csv")).toBe(false);
  });

  it("should resolve glob patterns to matching file paths", async () => {
    const globPattern = path.join(tmpDir, "logs", "*.csv");
    const matched = await resolveFilePatterns(globPattern);
    expect(matched.length).toBe(3);
    expect(matched[0]).toContain("part_1.csv");
    expect(matched[1]).toContain("part_2.csv");
    expect(matched[2]).toContain("part_3.csv");
  });

  it("should stream concatenate multiple files sequentially with correct offsets", async () => {
    const globPattern = path.join(tmpDir, "logs", "*.csv");
    const reader = new MultiFileReader(globPattern, { batchSize: 2 });
    const pipeline = createPipeline(reader);

    const rows = await pipeline.toArray();
    expect(rows.length).toBe(5);
    expect(rows[0]).toEqual({ id: "1", name: "Alice", value: "100" });
    expect(rows[4]).toEqual({ id: "5", name: "Eve", value: "500" });
  });

  it("should inject filename column when addFilename is enabled", async () => {
    const globPattern = path.join(tmpDir, "logs", "*.csv");
    const reader = new MultiFileReader(globPattern, { addFilename: true, batchSize: 10 });
    const pipeline = createPipeline(reader);

    const rows = await pipeline.toArray();
    expect(rows.length).toBe(5);
    expect(rows[0]!._file).toBe("part_1.csv");
    expect(rows[2]!._file).toBe("part_2.csv");
    expect(rows[4]!._file).toBe("part_3.csv");
  });

  it("should support custom filename column name via fileCol", async () => {
    const globPattern = path.join(tmpDir, "logs", "*.csv");
    const reader = new MultiFileReader(globPattern, { fileCol: "source_doc" });
    const pipeline = createPipeline(reader);

    const rows = await pipeline.toArray();
    expect(rows.length).toBe(5);
    expect(rows[0]!.source_doc).toBe("part_1.csv");
  });

  it("should automatically use MultiFileReader in createReader for glob string", async () => {
    const globPattern = path.join(tmpDir, "logs", "*.csv");
    const reader = createReader(globPattern);
    expect(reader).toBeInstanceOf(MultiFileReader);

    const pipeline = createPipeline(reader);
    const rows = await pipeline.toArray();
    expect(rows.length).toBe(5);
  });

  it("should respect maxRows across multiple files", async () => {
    const globPattern = path.join(tmpDir, "logs", "*.csv");
    const reader = new MultiFileReader(globPattern);
    const pipeline = createPipeline(reader, { maxRows: 3 });

    const rows = await pipeline.toArray();
    expect(rows.length).toBe(3);
    expect(rows[2]!.id).toBe("3");
  });
});
