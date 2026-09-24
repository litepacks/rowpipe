import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { promises as fs } from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import {
  pivotTransform,
  unpivotTransform,
  fuzzyJoinTransform,
  concatReaders,
  levenshteinDistance,
  levenshteinSimilarity,
  jaroSimilarity,
  jaroWinklerSimilarity,
  jaccardSimilarity,
  soundex,
  type DataBatch,
  type Row,
} from "../src/index.js";

const execAsync = promisify(exec);

async function collectStream(stream: AsyncIterable<DataBatch>): Promise<Row[]> {
  const rows: Row[] = [];
  for await (const batch of stream) {
    rows.push(...batch.rows);
  }
  return rows;
}

async function* createMockStream(rows: Row[]): AsyncIterable<DataBatch> {
  yield { rows, offset: 0 };
}

describe("Data Engineering & Reshaping Suite (Pivot, Unpivot, Fuzzy-Join, Concat)", () => {
  let tempDir: string;
  const binPath = path.resolve(__dirname, "../dist/cli/index.js");

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "rowpipe-de-test-"));
  });

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true });
  });

  describe("Pivot Transform", () => {
    const salesData: Row[] = [
      { region: "North", year: 2024, sales: 100, units: 10 },
      { region: "North", year: 2025, sales: 150, units: 12 },
      { region: "South", year: 2024, sales: 200, units: 20 },
      { region: "South", year: 2025, sales: 250, units: 22 },
      { region: "East", year: 2024, sales: 300, units: 30 },
    ];

    it("should pivot table with default sum aggregator", async () => {
      const transform = pivotTransform({
        index: "region",
        columns: "year",
        values: "sales",
        agg: "sum",
      });

      const result = await collectStream(transform(createMockStream(salesData)));
      expect(result.length).toBe(3);

      const north = result.find((r) => r["region"] === "North");
      expect(north).toBeDefined();
      expect(north!["2024"]).toBe(100);
      expect(north!["2025"]).toBe(150);

      const east = result.find((r) => r["region"] === "East");
      expect(east).toBeDefined();
      expect(east!["2024"]).toBe(300);
      expect(east!["2025"]).toBe(0); // filled with 0
    });

    it("should pivot table with average and custom fill value", async () => {
      const transform = pivotTransform({
        index: "region",
        columns: "year",
        values: "sales",
        agg: "avg",
        fill: "N/A",
      });

      const result = await collectStream(transform(createMockStream(salesData)));
      const east = result.find((r) => r["region"] === "East");
      expect(east!["2025"]).toBe("N/A");
    });

    it("should pivot table with composite index and min/max/count/first aggregators", async () => {
      const multiData: Row[] = [
        { dept: "Eng", role: "Dev", year: 2024, salary: 100 },
        { dept: "Eng", role: "Dev", year: 2024, salary: 120 },
        { dept: "Eng", role: "QA", year: 2024, salary: 80 },
      ];

      const maxTransform = pivotTransform({
        index: ["dept", "role"],
        columns: "year",
        values: "salary",
        agg: "max",
      });
      const maxRes = await collectStream(maxTransform(createMockStream(multiData)));
      const dev = maxRes.find((r) => r["dept"] === "Eng" && r["role"] === "Dev");
      expect(dev!["2024"]).toBe(120);

      const countTransform = pivotTransform({
        index: "dept",
        columns: "year",
        agg: "count",
      });
      const countRes = await collectStream(countTransform(createMockStream(multiData)));
      expect(countRes[0]!["2024"]).toBe(3);
    });
  });

  describe("Unpivot / Melt Transform", () => {
    const wideData: Row[] = [
      { id: "A", q1: 100, q2: 120, q3: 130, q4: null },
      { id: "B", q1: 200, q2: 210, q3: null, q4: 240 },
    ];

    it("should melt wide dataset into long format", async () => {
      const transform = unpivotTransform({
        index: "id",
        columns: ["q1", "q2", "q3", "q4"],
        varCol: "quarter",
        valCol: "revenue",
      });

      const result = await collectStream(transform(createMockStream(wideData)));
      expect(result.length).toBe(8);
      expect(result[0]).toEqual({ id: "A", quarter: "q1", revenue: 100 });
      expect(result[3]).toEqual({ id: "A", quarter: "q4", revenue: null });
    });

    it("should drop null values when dropNull is true", async () => {
      const transform = unpivotTransform({
        index: "id",
        dropNull: true,
      });

      const result = await collectStream(transform(createMockStream(wideData)));
      expect(result.length).toBe(6);
      expect(result.some((r) => r["value"] === null)).toBe(false);
    });
  });

  describe("Fuzzy Similarity Functions", () => {
    it("should compute accurate Levenshtein distances and similarity", () => {
      expect(levenshteinDistance("kitten", "sitting")).toBe(3);
      expect(levenshteinDistance("google", "google")).toBe(0);
      expect(levenshteinSimilarity("google", "google")).toBe(1.0);
      expect(levenshteinSimilarity("apple", "apply")).toBe(0.8);
    });

    it("should compute accurate Jaro and Jaro-Winkler similarities", () => {
      expect(jaroSimilarity("martha", "marhta")).toBeGreaterThan(0.9);
      expect(jaroWinklerSimilarity("dwayne", "duane")).toBeGreaterThan(0.8);
      expect(jaroWinklerSimilarity("same", "same")).toBe(1.0);
    });

    it("should compute accurate N-gram Jaccard similarities", () => {
      expect(jaccardSimilarity("night", "nacht")).toBeGreaterThan(0.0);
      expect(jaccardSimilarity("apple", "apple")).toBe(1.0);
    });

    it("should compute Soundex codes", () => {
      expect(soundex("Robert")).toBe("R163");
      expect(soundex("Rupert")).toBe("R163");
      expect(soundex("Rubin")).toBe("R150");
      expect(soundex("Smith")).toBe("S530");
      expect(soundex("Smythe")).toBe("S530");
      expect(soundex("Jackson")).toBe("J250");
    });
  });

  describe("Fuzzy Join Transform", () => {
    const leftRows: Row[] = [
      { id: 1, comp_name: "Apple Inc." },
      { id: 2, comp_name: "Micro soft Corp" },
      { id: 3, comp_name: "Amazon" },
      { id: 4, comp_name: "Unknown Entity" },
    ];

    const rightRows: Row[] = [
      { ticker: "AAPL", legal_name: "Apple Inc" },
      { ticker: "MSFT", legal_name: "Microsoft Corp." },
      { ticker: "AMZN", legal_name: "Amazon" },
    ];

    class MockReader {
      async *read() {
        yield { rows: rightRows, offset: 0 };
      }
    }

    it("should perform fuzzy left join and match slightly misspelled names", async () => {
      const transform = fuzzyJoinTransform({
        rightReader: new MockReader() as any,
        leftKey: "comp_name",
        rightKey: "legal_name",
        type: "left",
        threshold: 0.7,
        scoreCol: "sim_score",
      });

      const result = await collectStream(transform(createMockStream(leftRows)));
      expect(result.length).toBe(4);

      const apple = result.find((r) => r["id"] === 1);
      expect(apple!["ticker"]).toBe("AAPL");
      expect(apple!["sim_score"]).toBeGreaterThan(0.8);

      const msft = result.find((r) => r["id"] === 2);
      expect(msft!["ticker"]).toBe("MSFT");

      const unknown = result.find((r) => r["id"] === 4);
      expect(unknown!["ticker"]).toBeNull();
      expect(unknown!["sim_score"]).toBeNull();
    });

    it("should support inner fuzzy join", async () => {
      const transform = fuzzyJoinTransform({
        rightReader: new MockReader() as any,
        leftKey: "comp_name",
        rightKey: "legal_name",
        type: "inner",
        threshold: 0.7,
      });

      const result = await collectStream(transform(createMockStream(leftRows)));
      expect(result.length).toBe(3);
      expect(result.some((r) => r["id"] === 4)).toBe(false);
    });
  });

  describe("Concat Transform & Reader", () => {
    it("should concatenate multiple readers and tag source columns", async () => {
      const reader1 = {
        async *read() {
          yield { rows: [{ a: 1, b: 2 }], offset: 0 };
        },
      };
      const reader2 = {
        async *read() {
          yield { rows: [{ a: 3, b: 4, c: 5 }], offset: 0 };
        },
      };

      const concat = concatReaders([
        { reader: reader1 as any, label: "file1.csv" },
        { reader: reader2 as any, label: "file2.csv" },
      ], { sourceCol: "origin_file" });

      const result = await collectStream(concat.read());
      expect(result.length).toBe(2);
      expect(result[0]).toEqual({ a: 1, b: 2, origin_file: "file1.csv" });
      expect(result[1]).toEqual({ a: 3, b: 4, c: 5, origin_file: "file2.csv" });
    });
  });

  describe("CLI Command Integration", () => {
    it("should execute rowpipe pivot CLI command", async () => {
      const csvPath = path.join(tempDir, "sales.csv");
      await fs.writeFile(
        csvPath,
        "region,year,amount\nNorth,2024,100\nNorth,2025,150\nSouth,2024,200\nSouth,2025,250\n"
      );

      const { stdout } = await execAsync(
        `node "${binPath}" pivot "${csvPath}" --index region --columns year --values amount --json`
      );

      const parsed = JSON.parse(stdout);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(2);
      const north = parsed.find((r: any) => r.region === "North");
      expect(north["2024"]).toBe(100);
      expect(north["2025"]).toBe(150);
    });

    it("should execute rowpipe unpivot CLI command", async () => {
      const widePath = path.join(tempDir, "wide.csv");
      await fs.writeFile(
        widePath,
        "id,q1,q2\nprod_1,10,20\nprod_2,30,40\n"
      );

      const { stdout } = await execAsync(
        `node "${binPath}" unpivot "${widePath}" --index id --var-col quarter --val-col units --json`
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.length).toBe(4);
      expect(parsed[0]).toEqual({ id: "prod_1", quarter: "q1", units: "10" });
    });

    it("should execute rowpipe fuzzy-join CLI command", async () => {
      const leftPath = path.join(tempDir, "customers.csv");
      const rightPath = path.join(tempDir, "crm.csv");

      await fs.writeFile(leftPath, "id,name\n1,Jonathan Doe\n2,Alice Smith\n");
      await fs.writeFile(rightPath, "crm_id,full_name,vip\n101,John Doe,true\n102,Alice S.,false\n");

      const { stdout } = await execAsync(
        `node "${binPath}" fuzzy-join "${leftPath}" "${rightPath}" --left-key name --right-key full_name --threshold 0.6 --score-col score --json`
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.length).toBe(2);
      expect(String(parsed[0].crm_id)).toBe("101");
      expect(parsed[0].score).toBeGreaterThan(0.6);
    });

    it("should execute rowpipe concat CLI command", async () => {
      const file1 = path.join(tempDir, "log1.csv");
      const file2 = path.join(tempDir, "log2.csv");

      await fs.writeFile(file1, "time,msg\n10:00,start\n10:05,ok\n");
      await fs.writeFile(file2, "time,msg\n11:00,reboot\n");

      const { stdout } = await execAsync(
        `node "${binPath}" concat "${file1}" "${file2}" --source-col source --json`
      );

      const parsed = JSON.parse(stdout);
      expect(parsed.length).toBe(3);
      expect(parsed[0].source).toBe(file1);
      expect(parsed[2].source).toBe(file2);
    });
  });
});
