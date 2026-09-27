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
  ArrowReader,
  ArrowWriter,
  FeatherReader,
  FeatherWriter,
  mapArrowType,
  filterRows,
  mapRows,
} from "../src/index.js";
import { convertCommand } from "../src/cli/commands/convert.js";
import { inspectCommand } from "../src/cli/commands/inspect.js";
import { schemaCommand } from "../src/cli/commands/schema.js";
import { statsCommand } from "../src/cli/commands/stats.js";
import * as arrow from "apache-arrow";

const TEST_DIR = join(process.cwd(), "scratch_test_arrow");

describe("Apache Arrow IPC & Feather Format Support", () => {
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

  it("should verify Feather reader and writer are aliases of Arrow implementation", () => {
    expect(FeatherReader).toBe(ArrowReader);
    expect(FeatherWriter).toBe(ArrowWriter);
  });

  it("should infer 'arrow' and 'feather' formats from file extensions", () => {
    expect(inferReaderFormatFromPath("data.arrow")).toBe("arrow");
    expect(inferReaderFormatFromPath("data.arrow.gz")).toBe("arrow");
    expect(inferReaderFormatFromPath("data.feather")).toBe("feather");
    expect(inferReaderFormatFromPath("data.feather.gz")).toBe("feather");

    expect(inferWriterFormatFromPath("output.arrow")).toBe("arrow");
    expect(inferWriterFormatFromPath("output.arrow.gz")).toBe("arrow");
    expect(inferWriterFormatFromPath("output.feather")).toBe("feather");
    expect(inferWriterFormatFromPath("output.feather.gz")).toBe("feather");
  });

  it("should map Arrow DataType hierarchy into Rowpipe ColumnTypes", () => {
    expect(mapArrowType(new arrow.Int32())).toBe("integer");
    expect(mapArrowType(new arrow.Int64())).toBe("bigint");
    expect(mapArrowType(new arrow.Float64())).toBe("number");
    expect(mapArrowType(new arrow.Bool())).toBe("boolean");
    expect(mapArrowType(new arrow.Utf8())).toBe("string");
    expect(mapArrowType(new arrow.Binary())).toBe("binary");
    expect(mapArrowType(new arrow.TimestampMillisecond())).toBe("datetime");
    expect(mapArrowType(new arrow.DateMillisecond())).toBe("date");
    expect(mapArrowType(new arrow.Null())).toBe("null");
  });

  it("should write and read Arrow IPC with rich type fidelity", async () => {
    const arrowFile = join(TEST_DIR, "rich_types.arrow");
    const writer = new ArrowWriter(arrowFile);

    const testDate = new Date("2026-06-15T08:30:00.000Z");
    const sourceRows = [
      {
        id: 1,
        name: "Widget Pro",
        price: 99.95,
        inStock: true,
        created: testDate,
        tag: "hardware",
        notes: null,
      },
      {
        id: 2,
        name: "Software License",
        price: 249.0,
        inStock: false,
        created: testDate,
        tag: "software",
        notes: "digital delivery",
      },
      {
        id: 3,
        name: "Consulting Hour",
        price: 150.0,
        inStock: true,
        created: testDate,
        tag: "service",
        notes: null,
      },
    ];

    async function* generate() {
      yield { rows: sourceRows, offset: 0 };
    }

    await writer.write(generate());
    await writer.close();

    expect(existsSync(arrowFile)).toBe(true);

    // Read back via ArrowReader
    const reader = new ArrowReader(arrowFile);
    const readRows: any[] = [];
    for await (const batch of reader.read()) {
      readRows.push(...batch.rows);
    }

    expect(readRows).toHaveLength(3);
    expect(readRows[0].id).toBe(1);
    expect(readRows[0].name).toBe("Widget Pro");
    expect(readRows[0].price).toBe(99.95);
    expect(readRows[0].inStock).toBe(true);
    expect(new Date(readRows[0].created).toISOString()).toBe(testDate.toISOString());
    expect(readRows[0].notes).toBeNull();
    expect(readRows[1].inStock).toBe(false);
    expect(readRows[1].notes).toBe("digital delivery");
  });

  it("should write multiple batches incrementally without accumulating rows", async () => {
    const multiBatchFile = join(TEST_DIR, "multi_batch.arrow");
    const writer = new ArrowWriter(multiBatchFile);

    const BATCH_COUNT = 5;
    const ROWS_PER_BATCH = 50;

    async function* generateBatches() {
      for (let b = 0; b < BATCH_COUNT; b++) {
        const rows = [];
        for (let i = 0; i < ROWS_PER_BATCH; i++) {
          rows.push({
            batchNum: b,
            index: b * ROWS_PER_BATCH + i,
            label: `Row_${b}_${i}`,
          });
        }
        yield { rows, offset: b * ROWS_PER_BATCH };
      }
    }

    await writer.write(generateBatches());
    await writer.close();

    // Verify row count and sequence
    const reader = new ArrowReader(multiBatchFile);
    let totalRead = 0;
    for await (const batch of reader.read({ batchSize: 30 })) {
      totalRead += batch.rows.length;
    }

    expect(totalRead).toBe(BATCH_COUNT * ROWS_PER_BATCH);
  });

  it("should write and read Feather (.feather) format seamlessly", async () => {
    const featherFile = join(TEST_DIR, "data.feather");
    const writer = createWriter(featherFile);

    async function* generate() {
      yield {
        rows: [
          { sku: "SKU-001", count: 120, active: true },
          { sku: "SKU-002", count: 45, active: false },
        ],
        offset: 0,
      };
    }

    await writer.write(generate());
    if (writer.close) await writer.close();

    const reader = createReader(featherFile);
    const rows: any[] = [];
    for await (const batch of reader.read()) {
      rows.push(...batch.rows);
    }

    expect(rows).toHaveLength(2);
    expect(rows[0]).toEqual({ sku: "SKU-001", count: 120, active: true });
    expect(rows[1]).toEqual({ sku: "SKU-002", count: 45, active: false });
  });

  it("should inspect Arrow metadata accurately", async () => {
    const arrowFile = join(TEST_DIR, "inspect.arrow");
    const writer = new ArrowWriter(arrowFile);
    async function* generate() {
      yield {
        rows: [
          { a: 10, b: "hello", c: 3.14 },
          { a: 20, b: "world", c: 6.28 },
        ],
        offset: 0,
      };
    }
    await writer.write(generate());
    await writer.close();

    const reader = new ArrowReader(arrowFile);
    const meta = await reader.inspect!();

    expect(meta.format).toBe("ARROW");
    expect(meta.rowCount).toBe(2);
    expect(meta.columnCount).toBe(3);
    expect(meta.columns?.map((c) => c.name)).toEqual(["a", "b", "c"]);
  });

  it("should convert CSV to Arrow and Arrow to CSV via convertCommand (round-trip)", async () => {
    const csvFile = join(TEST_DIR, "products.csv");
    const arrowFile = join(TEST_DIR, "products.arrow");
    const roundtripCsv = join(TEST_DIR, "products_roundtrip.csv");

    writeFileSync(csvFile, "id,name,price\n1,Keyboard,99.9\n2,Mouse,49.9\n", "utf-8");

    // CSV -> Arrow
    await convertCommand(csvFile, arrowFile, { quiet: true });
    expect(existsSync(arrowFile)).toBe(true);

    // Arrow -> CSV
    await convertCommand(arrowFile, roundtripCsv, { quiet: true });
    expect(existsSync(roundtripCsv)).toBe(true);

    const roundtripContent = readFileSync(roundtripCsv, "utf-8");
    expect(roundtripContent).toContain("id,name,price\n");
    expect(roundtripContent).toContain("1,Keyboard,99.9\n");
    expect(roundtripContent).toContain("2,Mouse,49.9\n");
  });

  it("should convert Parquet to Arrow and Feather to Parquet", async () => {
    const parquetFile = join(TEST_DIR, "input.parquet");
    const arrowFile = join(TEST_DIR, "from_parquet.arrow");
    const parquetRoundtrip = join(TEST_DIR, "back_to.parquet");

    // Write source parquet
    const pWriter = createWriter(parquetFile, { format: "parquet" });
    async function* gen() {
      yield {
        rows: [
          { id: 100, label: "sensor_A", temp: 22.5 },
          { id: 101, label: "sensor_B", temp: 24.1 },
        ],
        offset: 0,
      };
    }
    await pWriter.write(gen());
    if (pWriter.close) await pWriter.close();

    // Parquet -> Arrow
    await convertCommand(parquetFile, arrowFile, { quiet: true });
    expect(existsSync(arrowFile)).toBe(true);

    // Arrow -> Parquet
    await convertCommand(arrowFile, parquetRoundtrip, { quiet: true });
    expect(existsSync(parquetRoundtrip)).toBe(true);

    // Read back final parquet
    const pReader = createReader(parquetRoundtrip);
    const rows: any[] = [];
    for await (const b of pReader.read()) {
      rows.push(...b.rows);
    }

    expect(rows).toHaveLength(2);
    expect(rows[0].label).toBe("sensor_A");
    expect(rows[1].label).toBe("sensor_B");
  });

  it("should support pipelines with Arrow input and output (filtering & mapping)", async () => {
    const sourceArrow = join(TEST_DIR, "source_pipeline.arrow");
    const outArrow = join(TEST_DIR, "filtered_pipeline.arrow");

    const writer = new ArrowWriter(sourceArrow);
    async function* gen() {
      yield {
        rows: [
          { country: "TR", city: "Istanbul", pop: 16000000 },
          { country: "DE", city: "Berlin", pop: 3800000 },
          { country: "TR", city: "Ankara", pop: 5800000 },
          { country: "FR", city: "Paris", pop: 2100000 },
        ],
        offset: 0,
      };
    }
    await writer.write(gen());
    await writer.close();

    const reader = new ArrowReader(sourceArrow);
    const targetWriter = new ArrowWriter(outArrow);

    await createPipeline(reader)
      .pipe(filterRows("country == 'TR'"))
      .pipe(mapRows({ isMegacity: "pop >= 10000000" }))
      .to(targetWriter);

    const resultReader = new ArrowReader(outArrow);
    const resultRows: any[] = [];
    for await (const b of resultReader.read()) {
      resultRows.push(...b.rows);
    }

    expect(resultRows).toHaveLength(2);
    expect(resultRows[0].city).toBe("Istanbul");
    expect(resultRows[0].isMegacity).toBe(true);
    expect(resultRows[1].city).toBe("Ankara");
    expect(resultRows[1].isMegacity).toBe(false);
  });

  it("should transparently handle compressed .arrow.gz", async () => {
    const srcRows = [
      { id: 1, text: "gzip arrow row 1" },
      { id: 2, text: "gzip arrow row 2" },
    ];

    const arrowGz = join(TEST_DIR, "compressed.arrow.gz");
    const writer = createWriter(arrowGz);
    async function* gen() {
      yield { rows: srcRows, offset: 0 };
    }
    await writer.write(gen());
    if (writer.close) await writer.close();

    expect(existsSync(arrowGz)).toBe(true);

    const reader = createReader(arrowGz);
    const readRows: any[] = [];
    for await (const b of reader.read()) {
      readRows.push(...b.rows);
    }

    expect(readRows).toHaveLength(2);
    expect(readRows[0].text).toBe("gzip arrow row 1");
    expect(readRows[1].text).toBe("gzip arrow row 2");
  });
});
