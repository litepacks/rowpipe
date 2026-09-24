import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { createReadStream, createWriteStream, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { gzipSync, gunzipSync } from "node:zlib";
import { join } from "node:path";
import {
  createPipeline,
  createReader,
  createWriter,
  CSVReader,
  CSVWriter,
  JSONLReader,
  JSONLWriter,
  MarkdownWriter,
  ParquetReader,
  ParquetWriter,
  mapRows,
  filterRows,
  reduceRows,
} from "../src/index.js";

const TEST_DIR = join(process.cwd(), "scratch_test_formats");

describe("New Formats & Streaming Extensions", () => {
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
  // 1. Transparent Gzip (.gz) Support
  // -------------------------------------------------------------
  describe("Transparent Gzip Compression (.gz)", () => {
    it("should transparently read .csv.gz files with stream decompression", async () => {
      const csvContent = "id,name,score\n1,Alice,95.5\n2,Bob,88.0\n3,Charlie,72.4\n";
      const gzBuffer = gzipSync(Buffer.from(csvContent, "utf-8"));
      const gzPath = join(TEST_DIR, "students.csv.gz");
      writeFileSync(gzPath, gzBuffer);

      const reader = createReader(gzPath);
      const rows: any[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ id: "1", name: "Alice", score: "95.5" });
      expect(rows[2].name).toBe("Charlie");
    });

    it("should transparently read and write .jsonl.gz files", async () => {
      const srcRows = [
        { city: "Istanbul", population: 15900000 },
        { city: "Ankara", population: 5800000 },
        { city: "Izmir", population: 4400000 },
      ];

      const outGzPath = join(TEST_DIR, "cities.jsonl.gz");
      const writer = createWriter(outGzPath);

      // Stream fake batches to writer
      async function* generate() {
        yield { rows: srcRows, offset: 0 };
      }
      await writer.write(generate());
      if (writer.close) await writer.close();

      expect(existsSync(outGzPath)).toBe(true);

      // Read back via JSONLReader
      const reader = createReader(outGzPath);
      const readRows: any[] = [];
      for await (const batch of reader.read()) {
        readRows.push(...batch.rows);
      }

      expect(readRows).toHaveLength(3);
      expect(readRows[0].city).toBe("Istanbul");
      expect(readRows[0].population).toBe(15900000);
      expect(readRows[1].city).toBe("Ankara");
    });
  });

  // -------------------------------------------------------------
  // 2. TSV & Custom Delimiters
  // -------------------------------------------------------------
  describe("TSV and Custom Delimiters", () => {
    it("should automatically detect and parse .tsv tab-separated files", async () => {
      const tsvContent = "gene\tchromosome\tposition\nTP53\tchr17\t7668402\nBRCA1\tchr17\t43044295\nEGFR\tchr7\t55019017\n";
      const tsvPath = join(TEST_DIR, "genes.tsv");
      writeFileSync(tsvPath, tsvContent, "utf-8");

      const reader = createReader(tsvPath);
      const rows: any[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ gene: "TP53", chromosome: "chr17", position: "7668402" });
      expect(rows[1].gene).toBe("BRCA1");
    });

    it("should write TSV format with tab delimiter", async () => {
      const tsvOutPath = join(TEST_DIR, "output_genes.tsv");
      const writer = createWriter(tsvOutPath, { format: "tsv" });

      async function* generate() {
        yield {
          rows: [
            { id: "G1", symbol: "MYC" },
            { id: "G2", symbol: "KRAS" },
          ],
          offset: 0,
        };
      }
      await writer.write(generate());
      if (writer.close) await writer.close();

      const content = readFileSync(tsvOutPath, "utf-8");
      expect(content).toContain("id\tsymbol\n");
      expect(content).toContain("G1\tMYC\n");
      expect(content).toContain("G2\tKRAS\n");
    });

    it("should parse PSV (pipe-separated values) with createReader", async () => {
      const psvContent = "id|name|role\n101|Ada|Admin\n102|Grace|Engineer\n";
      const psvPath = join(TEST_DIR, "users.psv");
      writeFileSync(psvPath, psvContent, "utf-8");

      const reader = createReader(psvPath);
      const rows: any[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(2);
      expect(rows[0]).toEqual({ id: "101", name: "Ada", role: "Admin" });
      expect(rows[1].name).toBe("Grace");
    });
  });

  // -------------------------------------------------------------
  // 3. Markdown Tables Writer (.md)
  // -------------------------------------------------------------
  describe("Markdown Tables Writer", () => {
    it("should stream rows into a GitHub-Flavored Markdown table", async () => {
      const mdPath = join(TEST_DIR, "report.md");
      const writer = new MarkdownWriter(mdPath);

      async function* generate() {
        yield {
          rows: [
            { product: "Laptop", category: "Electronics", price: 1200, rating: 4.8 },
            { product: "Desk Chair", category: "Furniture", price: 350, rating: 4.5 },
            { product: "Coffee Mug | Ceramic", category: "Kitchen\nUtensils", price: 15, rating: 4.9 },
          ],
          offset: 0,
        };
      }

      await writer.write(generate());
      if (writer.close) await writer.close();

      const content = readFileSync(mdPath, "utf-8");
      expect(content).toContain("product");
      expect(content).toContain("category");
      expect(content).toContain("price");
      expect(content).toContain("rating");
      expect(content).toContain("Laptop");
      // Verify pipe escaping
      expect(content).toContain("Coffee Mug \\| Ceramic");
      // Verify newline escaping to <br>
      expect(content).toContain("Kitchen<br>Utensils");
    });

    it("should support --to markdown in createWriter", async () => {
      const mdPath = join(TEST_DIR, "summary.md");
      const writer = createWriter(mdPath, { format: "markdown" });

      async function* generate() {
        yield {
          rows: [{ department: "Engineering", count: 42 }],
          offset: 0,
        };
      }
      await writer.write(generate());
      if (writer.close) await writer.close();

      const content = readFileSync(mdPath, "utf-8");
      expect(content).toContain("department");
      expect(content).toContain("count");
      expect(content).toContain("Engineering");
      expect(content).toContain("42");
    });
  });

  // -------------------------------------------------------------
  // 4. Apache Parquet (.parquet) Reader & Writer
  // -------------------------------------------------------------
  describe("Apache Parquet (.parquet)", () => {
    const parquetPath = join(TEST_DIR, "test_dataset.parquet");

    it("should write tabular stream to Apache Parquet with schema inference", async () => {
      const writer = new ParquetWriter(parquetPath);

      async function* generate() {
        yield {
          rows: [
            { id: 1, name: "Alpha", score: 88.5, active: true },
            { id: 2, name: "Beta", score: 92.0, active: false },
            { id: 3, name: "Gamma", score: 79.2, active: true },
          ],
          offset: 0,
        };
      }

      await writer.write(generate());
      if (writer.close) await writer.close();

      expect(existsSync(parquetPath)).toBe(true);
      expect(readFileSync(parquetPath).length).toBeGreaterThan(0);
    });

    it("should inspect Parquet metadata without loading full dataset", async () => {
      const reader = new ParquetReader(parquetPath);
      const metadata = await reader.inspect();

      expect(metadata.format).toBe("Parquet");
      expect(metadata.rowCount).toBe(3);
      expect(metadata.columnCount).toBe(4);
      expect(metadata.columns?.map((c) => c.name)).toEqual(["id", "name", "score", "active"]);
    });

    it("should stream and read rows from Parquet file", async () => {
      const reader = new ParquetReader(parquetPath);
      const rows: any[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual({ id: 1, name: "Alpha", score: 88.5, active: true });
      expect(rows[1]).toEqual({ id: 2, name: "Beta", score: 92.0, active: false });
      expect(rows[2].name).toBe("Gamma");
    });

    it("should pipeline transform directly from Parquet into Markdown and Gzipped CSV", async () => {
      const reader = new ParquetReader(parquetPath);
      const outCsvGz = join(TEST_DIR, "filtered_parquet.csv.gz");
      const writer = createWriter(outCsvGz);

      await createPipeline(reader)
        .pipe(filterRows("active == true && score >= 80"))
        .pipe(mapRows({ passed: "score >= 80" }))
        .to(writer);

      // Verify gzipped output
      const decompressed = gunzipSync(readFileSync(outCsvGz)).toString("utf-8");
      expect(decompressed).toContain("id,name,score,active,passed\n");
      expect(decompressed).toContain("1,Alpha,88.5,true,true\n");
      expect(decompressed).not.toContain("Beta"); // filtered out active == false
    });
  });
});
