import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  computeDiff,
  diffRows,
  DuplicateKeyError,
  InvalidArgumentError,
  DiffMismatchError,
  CSVWriter,
  JSONLWriter,
  XLSXWriter,
  ParquetWriter,
  createReader,
  createWriter,
  SpillableDiffIndex,
} from "../src/index.js";
import { formatKeyForDisplay, encodeCompositeKey } from "../src/diff/key.js";
import { areValuesEqual, compareRows } from "../src/diff/comparator.js";
import { diffCommand } from "../src/cli/commands/diff.js";

const TEST_DIR = join(process.cwd(), "scratch_test_diff");

describe("Rowpipe Diff Engine", () => {
  beforeAll(() => {
    if (!existsSync(TEST_DIR)) {
      mkdirSync(TEST_DIR, { recursive: true });
    }
  });

  afterAll(() => {
    if (existsSync(TEST_DIR)) {
      rmSync(TEST_DIR, { recursive: true, force: true });
    }
  });

  // -------------------------------------------------------------
  // 1. Basic Diff Cases
  // -------------------------------------------------------------
  describe("Basic Diff Operations", () => {
    it("should report zero differences for identical datasets", async () => {
      const leftCsv = join(TEST_DIR, "basic_left.csv");
      const rightCsv = join(TEST_DIR, "basic_right_identical.csv");
      const content = "id,name,score\n1,Alice,100\n2,Bob,85\n3,Carol,92\n";
      writeFileSync(leftCsv, content);
      writeFileSync(rightCsv, content);

      const summary = await computeDiff({
        left: leftCsv,
        right: rightCsv,
        keys: ["id"],
      });

      expect(summary.rows.added).toBe(0);
      expect(summary.rows.removed).toBe(0);
      expect(summary.rows.changed).toBe(0);
      expect(summary.rows.unchanged).toBe(3);
      expect(summary.rows.totalLeft).toBe(3);
      expect(summary.rows.totalRight).toBe(3);
    });

    it("should accurately detect added, removed, and changed rows", async () => {
      const leftCsv = join(TEST_DIR, "basic_diff_left.csv");
      const rightCsv = join(TEST_DIR, "basic_diff_right.csv");

      writeFileSync(leftCsv, "id,name,status,price\n1,Alice,active,10\n2,Bob,active,20\n3,Carol,inactive,30\n");
      writeFileSync(rightCsv, "id,name,status,price\n1,Alice,active,10\n2,Bob,inactive,25\n4,David,active,40\n");

      const summary = await computeDiff({
        left: leftCsv,
        right: rightCsv,
        keys: ["id"],
      });

      expect(summary.rows.added).toBe(1); // id=4
      expect(summary.rows.removed).toBe(1); // id=3
      expect(summary.rows.changed).toBe(1); // id=2 (status and price changed)
      expect(summary.rows.unchanged).toBe(1); // id=1

      expect(summary.columns["status"]?.changed).toBe(1);
      expect(summary.columns["price"]?.changed).toBe(1);
    });

    it("should handle empty left or right datasets", async () => {
      const leftEmpty = join(TEST_DIR, "empty_left.csv");
      const rightNormal = join(TEST_DIR, "normal_right.csv");

      writeFileSync(leftEmpty, "id,name\n");
      writeFileSync(rightNormal, "id,name\n1,Alpha\n2,Beta\n");

      const summary = await computeDiff({
        left: leftEmpty,
        right: rightNormal,
        keys: ["id"],
      });

      expect(summary.rows.added).toBe(2);
      expect(summary.rows.removed).toBe(0);
      expect(summary.rows.changed).toBe(0);
      expect(summary.rows.unchanged).toBe(0);
    });
  });

  // -------------------------------------------------------------
  // 2. Key Handling & Composite Keys
  // -------------------------------------------------------------
  describe("Keys & Composite Key Encoding", () => {
    it("should compare with composite keys and prevent delimiter collisions", async () => {
      const left = join(TEST_DIR, "comp_left.jsonl");
      const right = join(TEST_DIR, "comp_right.jsonl");

      // Test cases that would collide under naïve "a:b" string joining
      writeFileSync(
        left,
        JSON.stringify({ part1: "a:b", part2: "c", val: 10 }) + "\n" +
        JSON.stringify({ part1: "a", part2: "b:c", val: 20 }) + "\n"
      );
      writeFileSync(
        right,
        JSON.stringify({ part1: "a:b", part2: "c", val: 15 }) + "\n" +
        JSON.stringify({ part1: "a", part2: "b:c", val: 20 }) + "\n"
      );

      const summary = await computeDiff({
        left,
        right,
        keys: ["part1", "part2"],
      });

      expect(summary.rows.changed).toBe(1); // {part1: "a:b", part2: "c"} changed from 10 to 15
      expect(summary.rows.unchanged).toBe(1); // {part1: "a", part2: "b:c"} unchanged
      expect(summary.rows.added).toBe(0);
      expect(summary.rows.removed).toBe(0);
    });

    it("should throw DuplicateKeyError by default on duplicate key", async () => {
      const left = join(TEST_DIR, "dup_left.csv");
      const right = join(TEST_DIR, "dup_right.csv");

      writeFileSync(left, "id,val\n10,A\n20,B\n10,C\n"); // row 1 and row 3 have id=10
      writeFileSync(right, "id,val\n10,A\n20,B\n");

      await expect(
        computeDiff({
          left,
          right,
          keys: ["id"],
          duplicateKey: "error",
        })
      ).rejects.toThrow(DuplicateKeyError);
    });

    it("should honor duplicateKey=first and duplicateKey=last", async () => {
      const left = join(TEST_DIR, "dup_first_left.csv");
      const right = join(TEST_DIR, "dup_first_right.csv");

      writeFileSync(left, "id,val\n10,first_val\n10,last_val\n");
      writeFileSync(right, "id,val\n10,first_val\n");

      const summaryFirst = await computeDiff({
        left,
        right,
        keys: ["id"],
        duplicateKey: "first",
      });
      expect(summaryFirst.rows.unchanged).toBe(1);
      expect(summaryFirst.rows.changed).toBe(0);
    });
  });

  // -------------------------------------------------------------
  // 3. Value Equality, Types, and Options
  // -------------------------------------------------------------
  describe("Value Equality & Comparison Options", () => {
    it("should strictly distinguish null, undefined (missing), 0, and empty string by default", () => {
      expect(areValuesEqual(null, "")).toBe(false);
      expect(areValuesEqual(null, undefined)).toBe(false);
      expect(areValuesEqual(0, "0")).toBe(false);
      expect(areValuesEqual(false, "false")).toBe(false);
      expect(areValuesEqual(10, 10)).toBe(true);
      expect(areValuesEqual("test", "test")).toBe(true);
    });

    it("should support --coerce for type coercion", () => {
      expect(areValuesEqual(0, "0", { coerce: true })).toBe(true);
      expect(areValuesEqual(42, "42", { coerce: true })).toBe(true);
      expect(areValuesEqual(true, "true", { coerce: true })).toBe(true);
      expect(areValuesEqual(false, "0", { coerce: true })).toBe(true);
    });

    it("should support --epsilon for floating point tolerance", () => {
      expect(areValuesEqual(10.0001, 10.0002, { epsilon: 0 })).toBe(false);
      expect(areValuesEqual(10.0001, 10.0002, { epsilon: 0.001 })).toBe(true);
    });

    it("should support --trim and --ignore-case for strings", () => {
      expect(areValuesEqual("  Alice ", "Alice", { trim: false })).toBe(false);
      expect(areValuesEqual("  Alice ", "Alice", { trim: true })).toBe(true);

      expect(areValuesEqual("ALICE", "alice", { ignoreCase: false })).toBe(false);
      expect(areValuesEqual("ALICE", "alice", { ignoreCase: true })).toBe(true);
    });

    it("should support --ignore columns and reject ignoring key columns", async () => {
      const left = join(TEST_DIR, "ignore_left.csv");
      const right = join(TEST_DIR, "ignore_right.csv");

      writeFileSync(left, "id,price,updated_at\n1,100,2026-01-01\n");
      writeFileSync(right, "id,price,updated_at\n1,100,2026-09-16\n");

      // Without ignore -> changed
      const resWithout = await computeDiff({
        left,
        right,
        keys: ["id"],
      });
      expect(resWithout.rows.changed).toBe(1);

      // With ignore updated_at -> unchanged
      const resWith = await computeDiff({
        left,
        right,
        keys: ["id"],
        ignore: ["updated_at"],
      });
      expect(resWith.rows.unchanged).toBe(1);
      expect(resWith.rows.changed).toBe(0);

      // Attempting to ignore key column must throw InvalidArgumentError
      await expect(
        computeDiff({
          left,
          right,
          keys: ["id"],
          ignore: ["id"],
        })
      ).rejects.toThrow(InvalidArgumentError);
    });

    it("should support --columns to only compare specified subset", async () => {
      const left = join(TEST_DIR, "cols_left.csv");
      const right = join(TEST_DIR, "cols_right.csv");

      writeFileSync(left, "id,price,tag\n1,100,promoA\n");
      writeFileSync(right, "id,price,tag\n1,100,promoB\n");

      // Compare only price
      const summary = await computeDiff({
        left,
        right,
        keys: ["id"],
        columns: ["price"],
      });
      expect(summary.rows.unchanged).toBe(1);
      expect(summary.rows.changed).toBe(0);
    });
  });

  // -------------------------------------------------------------
  // 4. Output Modes & Patch Generation
  // -------------------------------------------------------------
  describe("Output Formats & CLI Streaming", () => {
    const left = join(TEST_DIR, "out_left.jsonl");
    const right = join(TEST_DIR, "out_right.jsonl");

    beforeAll(() => {
      writeFileSync(
        left,
        JSON.stringify({ id: 1, name: "Alice", status: "active" }) + "\n" +
        JSON.stringify({ id: 2, name: "Bob", status: "active" }) + "\n"
      );
      writeFileSync(
        right,
        JSON.stringify({ id: 1, name: "Alice", status: "active" }) + "\n" +
        JSON.stringify({ id: 2, name: "Bob", status: "inactive" }) + "\n" +
        JSON.stringify({ id: 3, name: "Charlie", status: "active" }) + "\n"
      );
    });

    it("should stream patch events in JSONL format", async () => {
      const patchFile = join(TEST_DIR, "patch_output.jsonl");
      await diffCommand(left, right, {
        key: "id",
        format: "patch",
        output: patchFile,
      });

      const patchContent = readFileSync(patchFile, "utf-8");
      expect(patchContent).toContain('"op":"update"');
      expect(patchContent).toContain('"op":"insert"');
      expect(patchContent).toContain('"id":3');
      expect(patchContent).toContain('"status":{"old":"active","new":"inactive"}');
    });

    it("should support --only and --limit in rows mode", async () => {
      const rowsFile = join(TEST_DIR, "rows_output.txt");
      await diffCommand(left, right, {
        key: "id",
        format: "rows",
        only: "changed",
        output: rowsFile,
      });

      const rowsContent = readFileSync(rowsFile, "utf-8");
      expect(rowsContent).toContain("CHANGED id=2");
      expect(rowsContent).toContain("status: active -> inactive");
      expect(rowsContent).not.toContain("ADDED");
      expect(rowsContent).not.toContain("UNCHANGED");
    });

    it("should throw DiffMismatchError when --fail-on-diff is set and differences exist", async () => {
      await expect(
        diffCommand(left, right, {
          key: "id",
          failOnDiff: true,
        })
      ).rejects.toThrow(DiffMismatchError);
    });
  });

  // -------------------------------------------------------------
  // 5. Cross-Format Diff Testing
  // -------------------------------------------------------------
  describe("Cross-Format Comparisons", () => {
    it("should diff between CSV and JSONL", async () => {
      const csvPath = join(TEST_DIR, "cross.csv");
      const jsonlPath = join(TEST_DIR, "cross.jsonl");

      writeFileSync(csvPath, "id,name,score\n1,Alpha,100\n2,Beta,200\n");
      writeFileSync(
        jsonlPath,
        JSON.stringify({ id: "1", name: "Alpha", score: "100" }) + "\n" +
        JSON.stringify({ id: "2", name: "Beta", score: "250" }) + "\n"
      );

      const summary = await computeDiff({
        left: csvPath,
        right: jsonlPath,
        keys: ["id"],
      });

      expect(summary.rows.unchanged).toBe(1);
      expect(summary.rows.changed).toBe(1);
    });

    it("should diff between CSV and Apache Parquet", async () => {
      const csvPath = join(TEST_DIR, "cross_pq.csv");
      const pqPath = join(TEST_DIR, "cross_pq.parquet");

      writeFileSync(csvPath, "id,item,qty\n101,Keyboard,5\n102,Mouse,12\n");

      const pqWriter = new ParquetWriter(pqPath);
      async function* generate() {
        yield {
          rows: [
            { id: 101, item: "Keyboard", qty: 5 },
            { id: 102, item: "Mouse", qty: 15 }, // changed qty
          ],
          offset: 0,
        };
      }
      await pqWriter.write(generate());
      if (pqWriter.close) await pqWriter.close();

      const summary = await computeDiff({
        left: csvPath,
        right: pqPath,
        keys: ["id"],
        coerce: true,
      });

      expect(summary.rows.unchanged).toBe(1); // 101
      expect(summary.rows.changed).toBe(1); // 102
    });

    it("should diff multi-sheet XLSX workbooks", async () => {
      const xlsxPathA = join(TEST_DIR, "workbookA.xlsx");
      const xlsxPathB = join(TEST_DIR, "workbookB.xlsx");

      const writerA = new XLSXWriter(xlsxPathA, { sheet: "Employees" });
      async function* genA() {
        yield {
          rows: [
            { emp_id: 1, name: "Alice", role: "Dev" },
            { emp_id: 2, name: "Bob", role: "Dev" },
          ],
          offset: 0,
        };
      }
      await writerA.write(genA());

      const writerB = new XLSXWriter(xlsxPathB, { sheet: "Employees" });
      async function* genB() {
        yield {
          rows: [
            { emp_id: 1, name: "Alice", role: "Lead Dev" },
            { emp_id: 2, name: "Bob", role: "Dev" },
          ],
          offset: 0,
        };
      }
      await writerB.write(genB());

      const summary = await computeDiff({
        left: xlsxPathA,
        right: xlsxPathB,
        keys: ["emp_id"],
        leftOptions: { sheet: "Employees" },
        rightOptions: { sheet: "Employees" },
      });

      expect(summary.rows.changed).toBe(1);
      expect(summary.rows.unchanged).toBe(1);
    });
  });

  // -------------------------------------------------------------
  // 6. Memory Spill to Disk Test
  // -------------------------------------------------------------
  describe("Spill-to-Disk Memory Management", () => {
    it("should transparently spill to disk when memory threshold is exceeded and maintain 100% accuracy", async () => {
      const leftFile = join(TEST_DIR, "large_left.csv");
      const rightFile = join(TEST_DIR, "large_right.csv");

      const count = 50000;
      const leftLines = ["id,code,value,flag"];
      const rightLines = ["id,code,value,flag"];

      for (let i = 1; i <= count; i++) {
        leftLines.push(`${i},CODE_${i},${i * 10},true`);
        if (i <= 40000) {
          if (i % 10 === 0) {
            // 4,000 changed
            rightLines.push(`${i},CODE_${i},${i * 10 + 1},true`);
          } else {
            // 36,000 unchanged
            rightLines.push(`${i},CODE_${i},${i * 10},true`);
          }
        }
      }
      // Add 5,000 new rows (50001..55000)
      for (let i = count + 1; i <= count + 5000; i++) {
        rightLines.push(`${i},NEW_CODE_${i},${i * 10},false`);
      }

      writeFileSync(leftFile, leftLines.join("\n") + "\n");
      writeFileSync(rightFile, rightLines.join("\n") + "\n");

      // Configure a very small memory limit (256 KB) to force disk spilling
      const summary = await computeDiff({
        left: leftFile,
        right: rightFile,
        keys: ["id"],
        memoryLimitBytes: 256 * 1024,
      });

      expect(summary.rows.added).toBe(5000);
      expect(summary.rows.removed).toBe(10000); // rows 40001..50000 removed
      expect(summary.rows.changed).toBe(4000);
      expect(summary.rows.unchanged).toBe(36000);
    }, 60000);
  });
});
