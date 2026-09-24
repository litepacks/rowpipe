import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { brotliCompressSync, brotliDecompressSync, gzipSync, deflateSync } from "node:zlib";
import { join } from "node:path";
import {
  createPipeline,
  createReader,
  createWriter,
  CSVReader,
  CSVWriter,
  JSONLReader,
  JSONLWriter,
  inferCompressionFromPath,
  isGzipPath,
  isBrotliPath,
  isZstdPath,
  isCompressedPath,
  stripCompressionExtension,
  ensureZstdInitialized,
  MultiFileReader,
} from "../src/index.js";
import { compress as compressZstd, init as initZstdWasm } from "@bokuweb/zstd-wasm";

const TEST_DIR = join(process.cwd(), "scratch_test_compression");

describe("Extended Compression Suite (.gz, .br, .zst, .zz)", () => {
  beforeAll(async () => {
    if (!existsSync(TEST_DIR)) {
      mkdirSync(TEST_DIR, { recursive: true });
    }
    await ensureZstdInitialized();
    await initZstdWasm();
  });

  afterAll(() => {
    if (existsSync(TEST_DIR)) {
      rmSync(TEST_DIR, { recursive: true, force: true });
    }
  });

  describe("Path & Format Inference", () => {
    it("correctly identifies compression types and strips extensions", () => {
      expect(inferCompressionFromPath("data.csv.gz")).toBe("gzip");
      expect(inferCompressionFromPath("data.csv.gzip")).toBe("gzip");
      expect(inferCompressionFromPath("data.jsonl.br")).toBe("brotli");
      expect(inferCompressionFromPath("data.jsonl.brotli")).toBe("brotli");
      expect(inferCompressionFromPath("data.csv.zst")).toBe("zstd");
      expect(inferCompressionFromPath("data.tsv.zstd")).toBe("zstd");
      expect(inferCompressionFromPath("data.csv.zz")).toBe("deflate");
      expect(inferCompressionFromPath("data.csv.deflate")).toBe("deflate");
      expect(inferCompressionFromPath("data.csv")).toBeNull();

      expect(isGzipPath("data.csv.gz")).toBe(true);
      expect(isBrotliPath("data.csv.br")).toBe(true);
      expect(isZstdPath("data.csv.zst")).toBe(true);
      expect(isCompressedPath("data.csv.zst")).toBe(true);
      expect(isCompressedPath("data.csv")).toBe(false);

      expect(stripCompressionExtension("data.csv.br")).toBe("data.csv");
      expect(stripCompressionExtension("data.jsonl.zst")).toBe("data.jsonl");
      expect(stripCompressionExtension("data.tsv.gz")).toBe("data.tsv");
    });
  });

  describe("Brotli Compression (.br / .brotli)", () => {
    it("transparently writes and reads .csv.br files", async () => {
      const csvPath = join(TEST_DIR, "output.csv.br");
      const sampleRows = [
        { id: 1, country: "Turkey", code: "TR", rank: 1 },
        { id: 2, country: "Germany", code: "DE", rank: 2 },
        { id: 3, country: "Japan", code: "JP", rank: 3 },
      ];

      // Write .csv.br
      const writer = createWriter(csvPath);
      const writePipeline = createPipeline(sampleRows);
      await writePipeline.to(writer);

      // Verify file is genuinely compressed Brotli
      const rawBytes = readFileSync(csvPath);
      const decompressedText = brotliDecompressSync(rawBytes).toString("utf-8");
      expect(decompressedText).toContain("Turkey");
      expect(decompressedText).toContain("Germany");

      // Read back with createReader
      const reader = createReader(csvPath);
      const readRows: any[] = [];
      for await (const batch of reader.read()) {
        readRows.push(...batch.rows);
      }

      expect(readRows).toHaveLength(3);
      expect(readRows[0].country).toBe("Turkey");
      expect(readRows[1].code).toBe("DE");
      expect(readRows[2].rank).toBe("3");
    });

    it("transparently writes and reads .jsonl.br files", async () => {
      const jsonlPath = join(TEST_DIR, "data.jsonl.br");
      const sampleRows = [
        { sku: "A100", price: 19.99, active: true },
        { sku: "B200", price: 49.50, active: false },
      ];

      const writer = createWriter(jsonlPath);
      await createPipeline(sampleRows).to(writer);

      const reader = createReader(jsonlPath);
      const readRows: any[] = [];
      for await (const batch of reader.read()) {
        readRows.push(...batch.rows);
      }

      expect(readRows).toHaveLength(2);
      expect(readRows[0]).toEqual({ sku: "A100", price: 19.99, active: true });
      expect(readRows[1]).toEqual({ sku: "B200", price: 49.50, active: false });
    });
  });

  describe("Zstandard Compression (.zst / .zstd)", () => {
    it("transparently writes and reads .csv.zst files", async () => {
      const zstPath = join(TEST_DIR, "output.csv.zst");
      const sampleRows = [
        { item_id: "X1", val: 100, tag: "alpha" },
        { item_id: "X2", val: 200, tag: "beta" },
        { item_id: "X3", val: 300, tag: "gamma" },
      ];

      // Write .csv.zst
      const writer = createWriter(zstPath);
      await createPipeline(sampleRows).to(writer);

      // Read back with createReader
      const reader = createReader(zstPath);
      const readRows: any[] = [];
      for await (const batch of reader.read()) {
        readRows.push(...batch.rows);
      }

      expect(readRows).toHaveLength(3);
      expect(readRows[0].item_id).toBe("X1");
      expect(readRows[0].val).toBe("100");
      expect(readRows[1].tag).toBe("beta");
      expect(readRows[2].val).toBe("300");
    });

    it("transparently writes and reads .jsonl.zst files with large data volume", async () => {
      const zstPath = join(TEST_DIR, "large_data.jsonl.zst");
      const sampleRows: any[] = [];
      for (let i = 0; i < 5000; i++) {
        sampleRows.push({
          id: i,
          uuid: `uuid-${i}-${Math.random().toString(36).substring(2)}`,
          amount: Math.round(i * 1.5 * 100) / 100,
          status: i % 2 === 0 ? "active" : "pending",
        });
      }

      // Write 5000 rows
      const writer = createWriter(zstPath);
      await createPipeline(sampleRows).to(writer);

      // Read back
      const reader = createReader(zstPath);
      const readRows: any[] = [];
      for await (const batch of reader.read()) {
        readRows.push(...batch.rows);
      }

      expect(readRows).toHaveLength(5000);
      expect(readRows[0].id).toBe(0);
      expect(readRows[4999].id).toBe(4999);
      expect(readRows[4999].status).toBe("pending");
    });
  });

  describe("Cross-format Conversion with Compression", () => {
    it("converts .csv.br directly to .jsonl.zst and then to .csv.gz", async () => {
      const inputBr = join(TEST_DIR, "pipeline_in.csv.br");
      const intermediateZst = join(TEST_DIR, "intermediate.jsonl.zst");
      const finalGz = join(TEST_DIR, "final.csv.gz");

      const rawCsv = "id,name,value\n1,Alpha,10\n2,Beta,20\n3,Gamma,30\n";
      writeFileSync(inputBr, brotliCompressSync(Buffer.from(rawCsv)));

      // 1. Convert .csv.br -> .jsonl.zst
      const reader1 = createReader(inputBr);
      const writer1 = createWriter(intermediateZst);
      await createPipeline(reader1).to(writer1);

      // 2. Convert .jsonl.zst -> .csv.gz
      const reader2 = createReader(intermediateZst);
      const writer2 = createWriter(finalGz);
      await createPipeline(reader2).to(writer2);

      // 3. Verify final output
      const finalReader = createReader(finalGz);
      const finalRows: any[] = [];
      for await (const batch of finalReader.read()) {
        finalRows.push(...batch.rows);
      }

      expect(finalRows).toHaveLength(3);
      expect(finalRows[0]).toEqual({ id: "1", name: "Alpha", value: "10" });
      expect(finalRows[1]).toEqual({ id: "2", name: "Beta", value: "20" });
      expect(finalRows[2]).toEqual({ id: "3", name: "Gamma", value: "30" });
    });
  });

  describe("MultiFileReader with Compressed Glob Patterns", () => {
    it("sequentially reads glob pattern of .csv.br files with injected filename", async () => {
      const subDir = join(TEST_DIR, "glob_br");
      mkdirSync(subDir, { recursive: true });

      const file1 = join(subDir, "part_01.csv.br");
      const file2 = join(subDir, "part_02.csv.br");

      writeFileSync(file1, brotliCompressSync(Buffer.from("id,val\n1,100\n2,200\n")));
      writeFileSync(file2, brotliCompressSync(Buffer.from("id,val\n3,300\n4,400\n")));

      const pattern = join(subDir, "*.csv.br");
      const multiReader = new MultiFileReader(pattern, { addFilename: true });

      const rows: any[] = [];
      for await (const batch of multiReader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows).toHaveLength(4);
      expect(rows[0]).toEqual({ _file: "part_01.csv.br", id: "1", val: "100" });
      expect(rows[1]).toEqual({ _file: "part_01.csv.br", id: "2", val: "200" });
      expect(rows[2]).toEqual({ _file: "part_02.csv.br", id: "3", val: "300" });
      expect(rows[3]).toEqual({ _file: "part_02.csv.br", id: "4", val: "400" });
    });
  });
});
