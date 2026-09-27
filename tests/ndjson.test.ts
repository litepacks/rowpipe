import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { gzipSync, gunzipSync } from "node:zlib";
import { join } from "node:path";
import {
  createPipeline,
  createReader,
  createWriter,
  inferReaderFormatFromPath,
  inferWriterFormatFromPath,
  JSONLReader,
  JSONLWriter,
  NDJSONReader,
  NDJSONWriter,
} from "../src/index.js";
import { inspectCommand } from "../src/cli/commands/inspect.js";
import { statsCommand } from "../src/cli/commands/stats.js";
import { convertCommand } from "../src/cli/commands/convert.js";

const TEST_DIR = join(process.cwd(), "scratch_test_ndjson");

describe("NDJSON Format Support & JSONL Alias Parity", () => {
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

  it("should verify NDJSONReader is identical to JSONLReader reference", () => {
    expect(NDJSONReader).toBe(JSONLReader);
    expect(NDJSONWriter).toBe(JSONLWriter);
  });

  it("should infer 'ndjson' format from .ndjson extension for reader and writer", () => {
    expect(inferReaderFormatFromPath("data.ndjson")).toBe("ndjson");
    expect(inferReaderFormatFromPath("data.ndjson.gz")).toBe("ndjson");
    expect(inferWriterFormatFromPath("data.ndjson")).toBe("ndjson");
    expect(inferWriterFormatFromPath("data.ndjson.gz")).toBe("ndjson");

    expect(inferReaderFormatFromPath("data.jsonl")).toBe("jsonl");
    expect(inferWriterFormatFromPath("data.jsonl")).toBe("jsonl");
  });

  it("should read and parse .ndjson identically to .jsonl", async () => {
    const records = [
      { id: 1, name: "Alpha", active: true },
      { id: 2, name: "Beta", active: false },
      { id: 3, name: "Gamma", active: true },
    ];
    const rawContent = records.map((r) => JSON.stringify(r)).join("\n") + "\n";

    const ndjsonPath = join(TEST_DIR, "items.ndjson");
    const jsonlPath = join(TEST_DIR, "items.jsonl");

    writeFileSync(ndjsonPath, rawContent, "utf-8");
    writeFileSync(jsonlPath, rawContent, "utf-8");

    // Read via ndjson inferred reader
    const ndjsonReader = createReader(ndjsonPath);
    const ndjsonRows: any[] = [];
    for await (const batch of ndjsonReader.read()) {
      ndjsonRows.push(...batch.rows);
    }

    // Read via jsonl inferred reader
    const jsonlReader = createReader(jsonlPath);
    const jsonlRows: any[] = [];
    for await (const batch of jsonlReader.read()) {
      jsonlRows.push(...batch.rows);
    }

    expect(ndjsonRows).toEqual(jsonlRows);
    expect(ndjsonRows).toEqual(records);
  });

  it("should support explicit --from ndjson and --from jsonl", async () => {
    const rawContent = '{"item":"widget","qty":10}\n{"item":"gadget","qty":25}\n';
    const filePath = join(TEST_DIR, "data_explicit.txt");
    writeFileSync(filePath, rawContent, "utf-8");

    const readerFromNdjson = createReader(filePath, { format: "ndjson" });
    const rowsNdjson: any[] = [];
    for await (const b of readerFromNdjson.read()) {
      rowsNdjson.push(...b.rows);
    }

    const readerFromJsonl = createReader(filePath, { format: "jsonl" });
    const rowsJsonl: any[] = [];
    for await (const b of readerFromJsonl.read()) {
      rowsJsonl.push(...b.rows);
    }

    expect(rowsNdjson).toEqual(rowsJsonl);
    expect(rowsNdjson).toEqual([
      { item: "widget", qty: 10 },
      { item: "gadget", qty: 25 },
    ]);
  });

  it("should write .ndjson identically to .jsonl", async () => {
    const testData = [
      { user: "alice", score: 99 },
      { user: "bob", score: 85 },
    ];

    const ndjsonOut = join(TEST_DIR, "out.ndjson");
    const jsonlOut = join(TEST_DIR, "out.jsonl");

    const ndjsonWriter = createWriter(ndjsonOut);
    const jsonlWriter = createWriter(jsonlOut);

    async function* gen() {
      yield { rows: testData, offset: 0 };
    }

    await ndjsonWriter.write(gen());
    if (ndjsonWriter.close) await ndjsonWriter.close();

    await jsonlWriter.write(gen());
    if (jsonlWriter.close) await jsonlWriter.close();

    const ndjsonText = readFileSync(ndjsonOut, "utf-8");
    const jsonlText = readFileSync(jsonlOut, "utf-8");

    expect(ndjsonText).toBe(jsonlText);
    expect(ndjsonText).toBe(
      '{"user":"alice","score":99}\n{"user":"bob","score":85}\n'
    );
  });

  it("should support explicit --to ndjson and --to jsonl", async () => {
    const testData = [{ key: "val1" }, { key: "val2" }];

    const ndjsonOut = join(TEST_DIR, "explicit_out_ndjson.txt");
    const jsonlOut = join(TEST_DIR, "explicit_out_jsonl.txt");

    const w1 = createWriter(ndjsonOut, { format: "ndjson" });
    const w2 = createWriter(jsonlOut, { format: "jsonl" });

    async function* gen() {
      yield { rows: testData, offset: 0 };
    }

    await w1.write(gen());
    if (w1.close) await w1.close();

    await w2.write(gen());
    if (w2.close) await w2.close();

    expect(readFileSync(ndjsonOut, "utf-8")).toBe(readFileSync(jsonlOut, "utf-8"));
  });

  it("should transparently handle compressed .ndjson.gz", async () => {
    const records = [{ id: 1, text: "compressed ndjson" }, { id: 2, text: "rowpipe streaming" }];
    const raw = records.map((r) => JSON.stringify(r)).join("\n") + "\n";
    const gz = gzipSync(Buffer.from(raw, "utf-8"));

    const gzFile = join(TEST_DIR, "compressed.ndjson.gz");
    writeFileSync(gzFile, gz);

    const reader = createReader(gzFile);
    const rows: any[] = [];
    for await (const b of reader.read()) {
      rows.push(...b.rows);
    }

    expect(rows).toEqual(records);

    // Write .ndjson.gz
    const outGz = join(TEST_DIR, "written.ndjson.gz");
    const writer = createWriter(outGz);
    async function* gen() {
      yield { rows, offset: 0 };
    }
    await writer.write(gen());
    if (writer.close) await writer.close();

    const decompressed = gunzipSync(readFileSync(outGz)).toString("utf-8");
    expect(decompressed).toBe(raw);
  });

  it("should convert CSV to NDJSON and NDJSON to CSV via convertCommand", async () => {
    const csvIn = join(TEST_DIR, "input.csv");
    const ndjsonOut = join(TEST_DIR, "converted.ndjson");
    const csvRoundtrip = join(TEST_DIR, "roundtrip.csv");

    writeFileSync(csvIn, "id,name,value\n1,Alpha,100\n2,Beta,200\n", "utf-8");

    // CSV -> NDJSON
    await convertCommand(csvIn, ndjsonOut, { quiet: true });
    expect(existsSync(ndjsonOut)).toBe(true);
    const ndjsonContent = readFileSync(ndjsonOut, "utf-8");
    expect(ndjsonContent).toBe(
      '{"id":"1","name":"Alpha","value":"100"}\n{"id":"2","name":"Beta","value":"200"}\n'
    );

    // NDJSON -> CSV
    await convertCommand(ndjsonOut, csvRoundtrip, { quiet: true });
    expect(existsSync(csvRoundtrip)).toBe(true);
    const roundtripContent = readFileSync(csvRoundtrip, "utf-8");
    expect(roundtripContent).toBe("id,name,value\n1,Alpha,100\n2,Beta,200\n");
  });

  it("should inspect NDJSON file and report correct metadata", async () => {
    const ndjsonPath = join(TEST_DIR, "inspect_test.ndjson");
    writeFileSync(ndjsonPath, '{"a":1,"b":"x"}\n{"a":2,"b":"y"}\n', "utf-8");

    const reader = createReader(ndjsonPath);
    const meta = await reader.inspect!();

    expect(meta.format).toBe("NDJSON");
    expect(meta.rowCount).toBe(2);
    expect(meta.columnCount).toBe(2);
    expect(meta.columns?.map((c) => c.name)).toEqual(["a", "b"]);
  });
});
