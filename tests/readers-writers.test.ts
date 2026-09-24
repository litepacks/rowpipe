import { describe, it, expect } from "vitest";
import { Readable, Writable } from "node:stream";
import { CSVReader } from "../src/readers/csv.js";
import { CSVWriter } from "../src/writers/csv.js";
import { JSONLReader } from "../src/readers/jsonl.js";
import { JSONLWriter } from "../src/writers/jsonl.js";
import { JSONReader } from "../src/readers/json.js";
import { JSONWriter } from "../src/writers/json.js";
import { XLSXReader } from "../src/readers/xlsx.js";
import { XLSXWriter } from "../src/writers/xlsx.js";
import { batchesToRows, rowsToBatches } from "../src/core/batch.js";
import type { DataBatch, Row } from "../src/core/types.js";
import { promises as fs } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";

async function collectRows(stream: AsyncIterable<DataBatch>): Promise<Row[]> {
  const results: Row[] = [];
  for await (const batch of stream) {
    results.push(...batch.rows);
  }
  return results;
}

describe("CSV Reader & Writer", () => {
  it("should read standard CSV with headers", async () => {
    const csvData = `id,name,age\n1,Alice,30\n2,Bob,25\n`;
    const reader = new CSVReader(Readable.from(csvData));
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([
      { id: "1", name: "Alice", age: "30" },
      { id: "2", name: "Bob", age: "25" },
    ]);
  });

  it("should handle custom delimiter and quotes with commas and newlines", async () => {
    const csvData = `"id";"name";"notes"\n"1";"Smith, John";"Line 1\nLine 2"\n"2";"Jane";"Simple"\n`;
    const reader = new CSVReader(Readable.from(csvData), { delimiter: ";" });
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([
      { id: "1", name: "Smith, John", notes: "Line 1\nLine 2" },
      { id: "2", name: "Jane", notes: "Simple" },
    ]);
  });

  it("should auto-detect delimiter when not specified", async () => {
    const tsvData = `id\tname\tage\n1\tAlice\t30\n2\tBob\t25\n`;
    const reader = new CSVReader(Readable.from(tsvData));
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([
      { id: "1", name: "Alice", age: "30" },
      { id: "2", name: "Bob", age: "25" },
    ]);
  });

  it("should handle UTF-8 BOM", async () => {
    const bomCsv = `\uFEFFid,name\n1,Alice\n`;
    const reader = new CSVReader(Readable.from(bomCsv));
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([{ id: "1", name: "Alice" }]);
  });

  it("should write CSV with proper escaping", async () => {
    const rows: Row[] = [
      { id: 1, name: "Alice, Bob", notes: 'He said "Hello"' },
      { id: 2, name: "Charlie", notes: "Line1\nLine2" },
    ];

    let output = "";
    const writable = new Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });

    const writer = new CSVWriter(writable);
    await writer.write(rowsToBatches(rows));

    expect(output).toContain('"Alice, Bob"');
    expect(output).toContain('"He said ""Hello"""');
    expect(output).toContain('"Line1\nLine2"');
  });
});

describe("JSONL Reader & Writer", () => {
  it("should stream read and write JSONL", async () => {
    const jsonlData = `{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n`;
    const reader = new JSONLReader(Readable.from(jsonlData));
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);

    let output = "";
    const writable = new Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });

    const writer = new JSONLWriter(writable);
    await writer.write(rowsToBatches(rows));

    expect(output).toBe(`{"id":1,"name":"Alice"}\n{"id":2,"name":"Bob"}\n`);
  });
});

describe("JSON Reader & Writer", () => {
  it("should streaming parse top-level JSON array", async () => {
    const jsonData = `[\n  {"id": 1, "name": "Alice"},\n  {"id": 2, "name": "Bob"}\n]`;
    const reader = new JSONReader(Readable.from(jsonData));
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ]);
  });

  it("should streaming parse nested path in JSON", async () => {
    const jsonData = `{\n  "status": "ok",\n  "data": {\n    "results": [\n      {"id": 1, "title": "First"},\n      {"id": 2, "title": "Second"}\n    ]\n  }\n}`;
    const reader = new JSONReader(Readable.from(jsonData), {
      path: "data.results",
    });
    const rows = await collectRows(reader.read());

    expect(rows).toEqual([
      { id: 1, title: "First" },
      { id: 2, title: "Second" },
    ]);
  });

  it("should write JSON array", async () => {
    const rows: Row[] = [
      { id: 1, name: "Alice" },
      { id: 2, name: "Bob" },
    ];

    let output = "";
    const writable = new Writable({
      write(chunk, _encoding, callback) {
        output += chunk.toString();
        callback();
      },
    });

    const writer = new JSONWriter(writable);
    await writer.write(rowsToBatches(rows));

    const parsed = JSON.parse(output);
    expect(parsed).toEqual(rows);
  });
});

describe("XLSX Reader & Writer", () => {
  it("should write and read back XLSX workbook", async () => {
    const tempFile = join(tmpdir(), `rowpipe_test_${Date.now()}.xlsx`);
    const initialRows: Row[] = [
      { id: 1, name: "Alice", score: 95.5 },
      { id: 2, name: "Bob", score: 82.0 },
    ];

    const writer = new XLSXWriter(tempFile, { sheet: "Students" });
    await writer.write(rowsToBatches(initialRows));
    await writer.close();

    const reader = new XLSXReader(tempFile, { sheet: "Students" });
    const rows = await collectRows(reader.read());

    expect(rows.length).toBe(2);
    expect(rows[0]!["name"]).toBe("Alice");
    expect(rows[1]!["name"]).toBe("Bob");

    // Inspect sheets
    const meta = await reader.inspect();
    expect(meta.sheets?.length).toBeGreaterThanOrEqual(1);
    expect(meta.sheets?.[0]?.name).toBe("Students");

    await fs.unlink(tempFile).catch(() => {});
  });
});
