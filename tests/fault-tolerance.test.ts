import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { CSVReader } from "../src/readers/csv.js";
import { JSONLReader } from "../src/readers/jsonl.js";
import { JSONReader } from "../src/readers/json.js";
import { mapRows } from "../src/transforms/map.js";
import { ParseError } from "../src/core/errors.js";
import { RowErrorHandler } from "../src/core/error-handler.js";
import { rowsToBatches } from "../src/core/batch.js";
import { Readable } from "node:stream";
import { promises as fs } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execAsync = promisify(execFile);
const ROWPIPE_CLI = join(__dirname, "../dist/cli/index.js");

describe("Fault-Tolerant Streaming (Error Handling & Dead-Letter Logging)", () => {
  let tempDir: string;

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(join(tmpdir(), "rowpipe-fault-test-"));
  });

  afterEach(async () => {
    try {
      await fs.rm(tempDir, { recursive: true, force: true });
    } catch {}
  });

  describe("CSVReader Fault Tolerance", () => {
    it("should throw ParseError on malformed CSV by default (abort strategy)", async () => {
      const corruptCsv = `id,name,val\n1,Alice,100\n2,Bob"unclosed,200\n3,Charlie,300\n`;
      const reader = new CSVReader(Readable.from(corruptCsv));

      await expect(async () => {
        for await (const batch of reader.read({ onError: "abort" })) {
          // should throw
        }
      }).rejects.toThrow(ParseError);
    });

    it("should skip corrupted CSV lines and yield valid rows with onError: 'skip'", async () => {
      const corruptCsv = `id,name,val\n1,Alice,100\n2,Bob"unclosed,200\n3,Charlie,300\n`;
      const reader = new CSVReader(Readable.from(corruptCsv));

      const rows: any[] = [];
      for await (const batch of reader.read({ onError: "skip" })) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(2);
      expect(rows[0]).toEqual({ id: "1", name: "Alice", val: "100" });
      expect(rows[1]).toEqual({ id: "3", name: "Charlie", val: "300" });
    });

    it("should log corrupted CSV lines to bad-rows-log with onError: 'log'", async () => {
      const corruptCsv = `id,name,val\n1,Alice,100\n2,Bob"badquote,200\n3,Charlie,300\n`;
      const badRowsLogPath = join(tempDir, "bad-rows.jsonl");

      const reader = new CSVReader(Readable.from(corruptCsv), { filePath: "corrupt.csv" });

      const rows: any[] = [];
      for await (const batch of reader.read({ onError: "log", badRowsLog: badRowsLogPath })) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(2);

      const logContent = await fs.readFile(badRowsLogPath, "utf-8");
      const logLines = logContent.trim().split("\n").map((l) => JSON.parse(l));

      expect(logLines).toHaveLength(1);
      expect(logLines[0].file).toBe("corrupt.csv");
      expect(logLines[0].errorType).toBe("ParseError");
    });
  });

  describe("JSONLReader Fault Tolerance", () => {
    it("should throw ParseError on invalid JSON line by default", async () => {
      const corruptJsonl = `{"id":1,"name":"Alice"}\nINVALID_JSON_ROW\n{"id":3,"name":"Charlie"}\n`;
      const reader = new JSONLReader(Readable.from(corruptJsonl));

      await expect(async () => {
        for await (const batch of reader.read({ onError: "abort" })) {
          // should throw
        }
      }).rejects.toThrow(ParseError);
    });

    it("should skip invalid JSON line and continue stream with onError: 'skip'", async () => {
      const corruptJsonl = `{"id":1,"name":"Alice"}\n{bad json}\n{"id":3,"name":"Charlie"}\n`;
      const reader = new JSONLReader(Readable.from(corruptJsonl));

      const rows: any[] = [];
      for await (const batch of reader.read({ onError: "skip" })) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(2);
      expect(rows[0].name).toBe("Alice");
      expect(rows[1].name).toBe("Charlie");
    });

    it("should write bad JSON lines to dead-letter log with onError: 'log'", async () => {
      const corruptJsonl = `{"id":1}\nNOT_JSON\n{"id":2}\n`;
      const badLog = join(tempDir, "jsonl-errors.jsonl");
      const reader = new JSONLReader(Readable.from(corruptJsonl), { filePath: "data.jsonl" });

      const rows: any[] = [];
      for await (const batch of reader.read({ onError: "log", badRowsLog: badLog })) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(2);

      const content = await fs.readFile(badLog, "utf-8");
      const entries = content.trim().split("\n").map((l) => JSON.parse(l));
      expect(entries).toHaveLength(1);
      expect(entries[0].raw).toBe("NOT_JSON");
      expect(entries[0].row).toBe(2);
    });
  });

  describe("JSONReader Fault Tolerance", () => {
    it("should skip malformed object in JSON array with onError: 'skip'", async () => {
      const corruptJson = `[{"id": 1, "name": "Alice"}, {malformed}, {"id": 3, "name": "Charlie"}]`;
      const reader = new JSONReader(Readable.from(corruptJson));

      const rows: any[] = [];
      for await (const batch of reader.read({ onError: "skip" })) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(1);
      expect(rows[1].id).toBe(3);
    });
  });

  describe("Transform Map Fault Tolerance", () => {
    it("should skip rows where custom mapper throws error", async () => {
      const source = [
        { id: 1, val: 10 },
        { id: 2, val: 0 },
        { id: 3, val: 20 },
      ];
      const stream = rowsToBatches(source);

      const mapper = mapRows(
        (row) => {
          if (row.val === 0) throw new Error("Division by zero error");
          return { ...row, ratio: 100 / (row.val as number) };
        },
        { onError: "skip" }
      );

      const result: any[] = [];
      for await (const batch of mapper(stream)) {
        result.push(...batch.rows);
      }

      expect(result).toHaveLength(2);
      expect(result[0].id).toBe(1);
      expect(result[1].id).toBe(3);
    });
  });

  describe("CLI Fault-Tolerant Conversion", () => {
    it("should convert corrupted CSV with --on-error skip and log bad rows with --bad-rows-log", async () => {
      const corruptCsvPath = join(tempDir, "input.csv");
      const outCsvPath = join(tempDir, "output.csv");
      const badRowsLogPath = join(tempDir, "bad.jsonl");

      await fs.writeFile(
        corruptCsvPath,
        "id,name,score\n1,Alice,95\n2,Bob\"corrupt,80\n3,Charlie,88\n"
      );

      const { stdout } = await execAsync(process.execPath, [
        ROWPIPE_CLI,
        "convert",
        corruptCsvPath,
        outCsvPath,
        "--on-error",
        "log",
        "--bad-rows-log",
        badRowsLogPath,
      ]);

      const outContent = await fs.readFile(outCsvPath, "utf-8");
      expect(outContent).toContain("1,Alice,95");
      expect(outContent).toContain("3,Charlie,88");
      expect(outContent).not.toContain("Bob");

      const badContent = await fs.readFile(badRowsLogPath, "utf-8");
      const badEntries = badContent.trim().split("\n").map((l) => JSON.parse(l));
      expect(badEntries.length).toBeGreaterThanOrEqual(1);
      expect(badEntries[0].errorType).toBe("ParseError");
    });
  });
});
