import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { technicalTransform } from "../src/analytics/technical.js";
import { computeKMeans, clusterAssignTransform } from "../src/analytics/cluster.js";
import { computeEntropy } from "../src/analytics/entropy.js";
import { computeNgrams } from "../src/analytics/ngrams.js";
import type { DataBatch } from "../src/core/types.js";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import * as fs from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";

const execAsync = promisify(exec);
const CLI_PATH = path.resolve(__dirname, "../dist/cli/index.js");

async function* createBatchIterator(rows: any[]): AsyncIterable<DataBatch> {
  yield {
    rows,
    offset: 0,
  };
}

async function collectBatches(iterator: AsyncIterable<DataBatch>): Promise<any[]> {
  const rows: any[] = [];
  for await (const batch of iterator) {
    rows.push(...batch.rows);
  }
  return rows;
}

describe("Finance, ML & NLP Suite (Technical Indicators, K-Means, Entropy, N-Grams)", () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "rowpipe-ml-test-"));
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  describe("technicalTransform (Financial Indicators)", () => {
    it("should calculate SMA, EMA, RSI, and Bollinger Bands", async () => {
      const prices = [10, 11, 12, 13, 14, 15, 14, 13, 12, 11];
      const rows = prices.map((p, idx) => ({ time: idx + 1, price: p, volume: 100 }));

      const stream = technicalTransform(createBatchIterator(rows), {
        priceCol: "price",
        volumeCol: "volume",
        indicators: "sma(3),ema(3),rsi(3),bollinger(3,2),vwap",
      });

      const results = await collectBatches(stream);
      expect(results.length).toBe(10);

      // SMA(3) at row 3 (10, 11, 12) = 11
      expect(results[0].sma_3).toBeNull();
      expect(results[1].sma_3).toBeNull();
      expect(results[2].sma_3).toBe(11);
      expect(results[3].sma_3).toBe(12);

      // VWAP for constant volume = cumulative average
      expect(results[0].vwap).toBe(10);
      expect(results[1].vwap).toBe(10.5);

      // Bollinger Bands at row 3
      expect(results[2].bb_3_2_mid).toBe(11);
      expect(results[2].bb_3_2_upper).toBeGreaterThan(11);
      expect(results[2].bb_3_2_lower).toBeLessThan(11);
    });
  });

  describe("computeKMeans (Mini-Batch K-Means)", () => {
    it("should cluster distinct 2D groups correctly", async () => {
      const rows: any[] = [];
      // Group 1 around (10, 10)
      for (let i = 0; i < 50; i++) {
        rows.push({ x: 10 + (Math.random() - 0.5), y: 10 + (Math.random() - 0.5) });
      }
      // Group 2 around (100, 100)
      for (let i = 0; i < 50; i++) {
        rows.push({ x: 100 + (Math.random() - 0.5), y: 100 + (Math.random() - 0.5) });
      }

      const result = await computeKMeans(createBatchIterator(rows), {
        cols: ["x", "y"],
        k: 2,
      });

      expect(result.k).toBe(2);
      expect(result.centroids.length).toBe(2);
      expect(result.total_samples).toBe(100);

      // Test cluster assignment transform
      const assignedStream = clusterAssignTransform(createBatchIterator(rows), result);
      const assignedRows = await collectBatches(assignedStream);

      expect(assignedRows.length).toBe(100);
      expect(assignedRows[0]._cluster_id).toBeDefined();
      expect(assignedRows[0]._cluster_distance).toBeDefined();
      expect(assignedRows[0]._cluster_id).not.toBe(assignedRows[99]._cluster_id);
    });
  });

  describe("computeEntropy (Shannon Entropy & Feature Importance)", () => {
    it("should compute entropy and information gain against target", async () => {
      // Predictable relationship: X1 perfectly predicts Y, X2 is random noise
      const rows = [
        { x1: "A", x2: "R1", y: "Yes" },
        { x1: "A", x2: "R2", y: "Yes" },
        { x1: "B", x2: "R1", y: "No" },
        { x1: "B", x2: "R2", y: "No" },
      ];

      const res = await computeEntropy(createBatchIterator(rows), {
        targetCol: "y",
      });

      expect(res.target_entropy).toBeCloseTo(1.0, 2); // 50/50 binary entropy = 1 bit
      expect(res.features.length).toBe(2);

      const fX1 = res.features.find((f) => f.feature === "x1");
      const fX2 = res.features.find((f) => f.feature === "x2");

      expect(fX1).toBeDefined();
      expect(fX2).toBeDefined();
      expect(fX1!.information_gain).toBeCloseTo(1.0, 2); // Perfect information gain
      expect(fX2!.information_gain).toBeCloseTo(0.0, 2); // Zero gain
    });
  });

  describe("computeNgrams (Text N-Gram Extraction)", () => {
    it("should extract bigrams and unigrams with counts and percentages", async () => {
      const rows = [
        { text: "quick brown fox jumps over lazy dog" },
        { text: "quick brown fox jumps again" },
      ];

      const res = await computeNgrams(createBatchIterator(rows), {
        col: "text",
        n: 2,
        top: 5,
      });

      expect(res.n).toBe(2);
      expect(res.total_ngrams).toBe(10);
      expect(res.items.length).toBeGreaterThan(0);

      const topBigram = res.items[0];
      expect(topBigram!.ngram).toBe("quick brown");
      expect(topBigram!.count).toBe(2);
    });

    it("should filter stopwords when enabled", async () => {
      const rows = [
        { text: "the dog and the cat with the fox" },
      ];

      const res = await computeNgrams(createBatchIterator(rows), {
        col: "text",
        n: 1,
        stopwords: true,
      });

      const ngrams = res.items.map((it) => it.ngram);
      expect(ngrams).not.toContain("the");
      expect(ngrams).not.toContain("and");
      expect(ngrams).not.toContain("with");
      expect(ngrams).toContain("dog");
      expect(ngrams).toContain("cat");
      expect(ngrams).toContain("fox");
    });
  });

  describe("CLI Command Integration", () => {
    it("should execute rowpipe technical CLI", async () => {
      const csvPath = path.join(tmpDir, "stock.csv");
      await fs.writeFile(
        csvPath,
        "date,close,vol\n2024-01-01,100,500\n2024-01-02,105,600\n2024-01-03,110,700\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} technical ${csvPath} --price close --indicators "sma(2),vwap" --json`);
      const parsed = JSON.parse(stdout);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(3);
      expect(parsed[1].sma_2).toBe(102.5);
    });

    it("should execute rowpipe cluster CLI with summary", async () => {
      const csvPath = path.join(tmpDir, "pts.csv");
      await fs.writeFile(
        csvPath,
        "x,y\n1,1\n2,2\n3,3\n10,10\n11,11\n12,12\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} cluster ${csvPath} --cols x,y --k 2 --summary --json`);
      const parsed = JSON.parse(stdout);
      expect(parsed.k).toBe(2);
      expect(parsed.centroids.length).toBe(2);
    });

    it("should execute rowpipe entropy CLI", async () => {
      const csvPath = path.join(tmpDir, "churn.csv");
      await fs.writeFile(
        csvPath,
        "age,income,churn\n25,50000,0\n45,100000,1\n26,52000,0\n50,110000,1\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} entropy ${csvPath} --target churn --json`);
      const parsed = JSON.parse(stdout);
      expect(parsed.target_column).toBe("churn");
      expect(parsed.features.length).toBe(2);
    });

    it("should execute rowpipe ngrams CLI", async () => {
      const csvPath = path.join(tmpDir, "reviews.csv");
      await fs.writeFile(
        csvPath,
        "id,feedback\n1,great product very fast\n2,great product highly recommend\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} ngrams ${csvPath} feedback --n 2 --json`);
      const parsed = JSON.parse(stdout);
      expect(parsed.n).toBe(2);
      expect(parsed.items[0].ngram).toBe("great product");
      expect(parsed.items[0].count).toBe(2);
    });
  });
});
