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
  AvroReader,
  AvroWriter,
  inferAvroSchema,
  mapAvroType,
  normalizeAvroValue,
  filterRows,
  mapRows,
} from "../src/index.js";
import { convertCommand } from "../src/cli/commands/convert.js";
import { inspectCommand } from "../src/cli/commands/inspect.js";
import { schemaCommand } from "../src/cli/commands/schema.js";
import { statsCommand } from "../src/cli/commands/stats.js";

const TEST_DIR = join(process.cwd(), "scratch_test_avro");

describe("Apache Avro Format Support", () => {
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

  it("should infer 'avro' format from file extensions", () => {
    expect(inferReaderFormatFromPath("data.avro")).toBe("avro");
    expect(inferReaderFormatFromPath("data.avro.gz")).toBe("avro");
    expect(inferWriterFormatFromPath("output.avro")).toBe("avro");
    expect(inferWriterFormatFromPath("output.avro.gz")).toBe("avro");
  });

  it("should map Avro schema types into Rowpipe ColumnTypes", () => {
    expect(mapAvroType("null")).toBe("null");
    expect(mapAvroType("boolean")).toBe("boolean");
    expect(mapAvroType("int")).toBe("integer");
    expect(mapAvroType("long")).toBe("bigint");
    expect(mapAvroType({ type: "long", logicalType: "timestamp-millis" })).toBe("datetime");
    expect(mapAvroType("float")).toBe("number");
    expect(mapAvroType("double")).toBe("number");
    expect(mapAvroType("bytes")).toBe("binary");
    expect(mapAvroType("string")).toBe("string");
    expect(mapAvroType({ type: "array", items: "string" })).toBe("json");
    expect(mapAvroType({ type: "map", values: "int" })).toBe("json");
    expect(mapAvroType(["null", "string"])).toBe("string");
    expect(mapAvroType(["null", "int", "string"])).toBe("mixed");
  });

  it("should write and read pure Avro container files with rich types", async () => {
    const filePath = join(TEST_DIR, "rich_types.avro");
    const writer = new AvroWriter(filePath);

    const rows = [
      {
        id: 1,
        name: "Alice",
        age: 30,
        active: true,
        score: 98.5,
        tags: ["admin", "staff"],
        meta: { role: "lead" },
        joined_at: new Date("2025-01-01T12:00:00Z"),
        avatar: Buffer.from("fake-png-data"),
      },
      {
        id: 2,
        name: "Bob",
        age: null,
        active: false,
        score: 72.0,
        tags: ["user"],
        meta: { role: "guest" },
        joined_at: new Date("2025-02-15T08:30:00Z"),
        avatar: null,
      },
    ];

    await writer.write(rows);
    await writer.close();

    // Inspect
    const reader = new AvroReader(filePath);
    const inspection = await reader.inspect();
    expect(inspection.format).toBe("avro");
    expect(inspection.columns.length).toBeGreaterThan(5);
    const colNames = inspection.columns.map((c) => c.name);
    expect(colNames).toContain("id");
    expect(colNames).toContain("name");
    expect(colNames).toContain("age");
    expect(colNames).toContain("avatar");

    // Read rows back
    const readRows: any[] = [];
    for await (const batch of reader.read()) {
      readRows.push(...batch.rows);
    }

    expect(readRows).toHaveLength(2);
    expect(readRows[0].id).toBe(1);
    expect(readRows[0].name).toBe("Alice");
    expect(readRows[0].active).toBe(true);
    expect(readRows[0].score).toBeCloseTo(98.5);
    expect(readRows[0].tags).toEqual(["admin", "staff"]);
    expect(readRows[0].meta).toEqual({ role: "lead" });
    expect(readRows[0].avatar).toBeInstanceOf(Buffer);

    expect(readRows[1].id).toBe(2);
    expect(readRows[1].name).toBe("Bob");
    expect(readRows[1].age).toBeNull();
    expect(readRows[1].active).toBe(false);
    expect(readRows[1].avatar).toBeNull();
  });

  it("should infer nullable union schema for schemaless conversion", () => {
    const sample = [
      { id: 1, name: "Alice", note: null },
      { id: 2, name: null, note: "hello" },
    ];
    const schema = inferAvroSchema(sample);
    expect(schema.type).toBe("record");
    expect(schema.fields).toHaveLength(3);

    const nameField = schema.fields.find((f: any) => f.name === "name");
    expect(nameField.type).toEqual(["null", "string"]);
    expect(nameField.default).toBeNull();
  });

  it("should stream convert CSV to Avro and Avro back to CSV", async () => {
    const csvContent = "id,name,amount,active\n1,Widget,19.99,true\n2,Gadget,49.50,false\n3,Doodad,,true\n";
    const csvPath = join(TEST_DIR, "products.csv");
    const avroPath = join(TEST_DIR, "products.avro");
    const roundtripCsvPath = join(TEST_DIR, "products_back.csv");

    writeFileSync(csvPath, csvContent, "utf8");

    // Convert CSV -> Avro
    await convertCommand(csvPath, avroPath, { quiet: true });
    expect(existsSync(avroPath)).toBe(true);

    // Convert Avro -> CSV
    await convertCommand(avroPath, roundtripCsvPath, { quiet: true });

    const outputCsv = readFileSync(roundtripCsvPath, "utf8");
    expect(outputCsv).toContain("Widget");
    expect(outputCsv).toContain("Gadget");
    expect(outputCsv).toContain("Doodad");
  });

  it("should round-trip JSONL -> Avro -> JSONL preserving structure", async () => {
    const jsonlData = [
      { id: 101, city: "Istanbul", population: 15840000, coastal: true },
      { id: 102, city: "Ankara", population: 5747000, coastal: false },
      { id: 103, city: "Izmir", population: 4425000, coastal: true },
    ];
    const jsonlPath = join(TEST_DIR, "cities.jsonl");
    const avroPath = join(TEST_DIR, "cities.avro");
    const backJsonlPath = join(TEST_DIR, "cities_back.jsonl");

    writeFileSync(jsonlPath, jsonlData.map((d) => JSON.stringify(d)).join("\n") + "\n", "utf8");

    // JSONL -> Avro
    await convertCommand(jsonlPath, avroPath, { quiet: true });

    // Avro -> JSONL
    await convertCommand(avroPath, backJsonlPath, { quiet: true });

    const recovered = readFileSync(backJsonlPath, "utf8")
      .trim()
      .split("\n")
      .map((l) => JSON.parse(l));

    expect(recovered).toHaveLength(3);
    expect(recovered[0]).toEqual(jsonlData[0]);
    expect(recovered[1]).toEqual(jsonlData[1]);
    expect(recovered[2]).toEqual(jsonlData[2]);
  });

  it("should convert Avro to Parquet cleanly", async () => {
    const avroPath = join(TEST_DIR, "cities.avro");
    const parquetPath = join(TEST_DIR, "cities.parquet");

    await convertCommand(avroPath, parquetPath, { quiet: true });

    expect(existsSync(parquetPath)).toBe(true);

    const parquetReader = createReader(parquetPath);
    const rows: any[] = [];
    for await (const b of parquetReader.read()) {
      rows.push(...b.rows);
    }
    expect(rows).toHaveLength(3);
    expect(rows[0].city).toBe("Istanbul");
  });

  it("should handle GZIP-compressed Avro files (.avro.gz)", async () => {
    const avroPath = join(TEST_DIR, "cities.avro");
    const avroBytes = readFileSync(avroPath);
    const gzPath = join(TEST_DIR, "cities.avro.gz");
    writeFileSync(gzPath, gzipSync(avroBytes));

    const reader = new AvroReader(gzPath);
    const rows: any[] = [];
    for await (const batch of reader.read()) {
      rows.push(...batch.rows);
    }
    expect(rows).toHaveLength(3);
    expect(rows[1].city).toBe("Ankara");
  });

  it("should report detailed, actionable schema validation errors with field, expected type, and row", async () => {
    const avroPath = join(TEST_DIR, "strict.avro");

    // Explicit strict Avro schema: id is integer, name is string
    const strictSchema = {
      type: "record",
      name: "StrictUser",
      fields: [
        { name: "id", type: "int" },
        { name: "name", type: "string" },
      ],
    };

    const writer = new AvroWriter(avroPath, {
      schema: strictSchema,
    });

    const badRows = [
      { id: 1, name: "Alice" },
      { id: 2, name: 999999 }, // name should be string!
    ];

    await expect(writer.write(badRows)).rejects.toThrow(/Avro schema validation failed at record 2: field "name"/);
  });

  it("should support transforms and filtering in Avro pipelines", async () => {
    const avroPath = join(TEST_DIR, "cities.avro");
    const filteredJsonlPath = join(TEST_DIR, "cities_filtered.jsonl");

    await createPipeline(createReader(avroPath))
      .pipe(filterRows("coastal == true"))
      .pipe(mapRows({ country: "'Turkey'" }))
      .to(createWriter(filteredJsonlPath));

    const results = readFileSync(filteredJsonlPath, "utf8")
      .trim()
      .split("\n")
      .map((l) => JSON.parse(l));

    expect(results).toHaveLength(2);
    expect(results.every((r) => r.coastal === true)).toBe(true);
    expect(results[0].country).toBe("Turkey");
  });

  it("should run CLI commands (inspect, schema, stats, convert) on Avro files", async () => {
    const avroPath = join(TEST_DIR, "cities.avro");

    // CLI inspect
    await expect(inspectCommand(avroPath, { json: true })).resolves.not.toThrow();

    // CLI schema
    await expect(schemaCommand(avroPath, { json: true })).resolves.not.toThrow();

    // CLI stats
    await expect(statsCommand(avroPath, { json: true })).resolves.not.toThrow();

    // CLI convert
    const outCsv = join(TEST_DIR, "cities_cli.csv");
    await expect(convertCommand(avroPath, outCsv, { quiet: true })).resolves.not.toThrow();
    expect(existsSync(outCsv)).toBe(true);
  });
});
