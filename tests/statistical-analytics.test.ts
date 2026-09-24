import { describe, it, expect } from "vitest";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import { computeCorrelationMatrix, formatCorrelationRows } from "../src/analytics/correlation.js";
import { computeQuantiles } from "../src/analytics/quantiles.js";
import { outliersTransform } from "../src/analytics/outliers.js";
import { computeCrosstab, formatCrosstabRows } from "../src/analytics/crosstab.js";
import { computeLinearRegression } from "../src/analytics/regression.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { Row } from "../src/core/types.js";

const execAsync = promisify(exec);
const cliPath = join(process.cwd(), "dist/cli/index.js");

describe("Statistical Analytics Engine Suite", () => {
  describe("computeCorrelationMatrix (Pearson Correlation)", () => {
    it("should calculate correct correlation for perfectly correlated variables", async () => {
      const rows: Row[] = [
        { a: 1, b: 2, c: 10 },
        { a: 2, b: 4, c: 8 },
        { a: 3, b: 6, c: 6 },
        { a: 4, b: 8, c: 4 },
        { a: 5, b: 10, c: 2 },
      ];

      const pipeline = createPipeline(rows);
      const result = await computeCorrelationMatrix(pipeline.batches(), ["a", "b", "c"]);

      expect(result.sampleSize).toBe(5);
      expect(result.matrix["a"]!["a"]).toBe(1);
      expect(result.matrix["a"]!["b"]).toBe(1); // perfectly positive
      expect(result.matrix["a"]!["c"]).toBe(-1); // perfectly negative
      expect(result.matrix["b"]!["c"]).toBe(-1);

      const formatted = formatCorrelationRows(result);
      expect(formatted).toHaveLength(3);
      expect(formatted[0]!["column"]).toBe("a");
    });
  });

  describe("computeQuantiles (Percentiles & IQR)", () => {
    it("should compute exact quantiles, median, and IQR", async () => {
      // 1 to 100
      const rows: Row[] = [];
      for (let i = 1; i <= 100; i++) {
        rows.push({ val: i });
      }

      const pipeline = createPipeline(rows);
      const result = await computeQuantiles(pipeline.batches(), "val", [50, 90, 95, 99]);

      expect(result.count).toBe(100);
      expect(result.min).toBe(1);
      expect(result.max).toBe(100);
      expect(result.median).toBe(50.5);
      expect(result.q1).toBe(25.75);
      expect(result.q3).toBe(75.25);
      expect(result.iqr).toBe(49.5);
      expect(result.percentiles["p90"]).toBe(90.1);
      expect(result.percentiles["p99"]).toBe(99.01);
    });
  });

  describe("outliersTransform (Anomaly Detection)", () => {
    it("should detect statistical outliers using Z-Score", async () => {
      const rows: Row[] = [
        { id: 1, amount: 10 },
        { id: 2, amount: 12 },
        { id: 3, amount: 11 },
        { id: 4, amount: 10 },
        { id: 5, amount: 12 },
        { id: 6, amount: 11 },
        { id: 7, amount: 10 },
        { id: 8, amount: 12 },
        { id: 9, amount: 11 },
        { id: 10, amount: 1000 }, // Clear outlier
      ];

      const pipeline = createPipeline(rows).pipe(
        outliersTransform({
          column: "amount",
          method: "zscore",
          threshold: 2.5,
          onlyOutliers: true,
        })
      );

      const result = await pipeline.toArray();
      expect(result).toHaveLength(1);
      expect(result[0]!["id"]).toBe(10);
      expect(result[0]!["amount"]).toBe(1000);
    });

    it("should append outlier annotation columns when addColumns is enabled", async () => {
      const rows: Row[] = [
        { id: 1, val: 5 },
        { id: 2, val: 500 },
      ];

      const pipeline = createPipeline(rows).pipe(
        outliersTransform({
          column: "val",
          method: "zscore",
          threshold: 1.0,
          addColumns: true,
        })
      );

      const result = await pipeline.toArray();
      expect(result).toHaveLength(2);
      expect(result[0]).toHaveProperty("_is_outlier");
      expect(result[0]).toHaveProperty("_outlier_score");
    });
  });

  describe("computeCrosstab (2D Contingency Matrix)", () => {
    it("should compute 2-way frequency crosstab with row/col totals", async () => {
      const rows: Row[] = [
        { genre: "Action", rating: "PG" },
        { genre: "Action", rating: "R" },
        { genre: "Action", rating: "R" },
        { genre: "Comedy", rating: "PG" },
        { genre: "Comedy", rating: "PG" },
      ];

      const pipeline = createPipeline(rows);
      const result = await computeCrosstab(pipeline.batches(), "genre", "rating");

      expect(result.grandTotal).toBe(5);
      expect(result.rowTotals["Action"]).toBe(3);
      expect(result.rowTotals["Comedy"]).toBe(2);
      expect(result.colTotals["PG"]).toBe(3);
      expect(result.colTotals["R"]).toBe(2);
      expect(result.counts["Action"]!["R"]).toBe(2);

      const formatted = formatCrosstabRows(result);
      expect(formatted.length).toBe(3); // Action, Comedy, Total
      expect(formatted[2]!["genre"]).toBe("Total");
    });
  });

  describe("computeLinearRegression (OLS Regression)", () => {
    it("should compute slope, intercept, and R-squared for linear relationship", async () => {
      // y = 2x + 3
      const rows: Row[] = [
        { x: 1, y: 5 },
        { x: 2, y: 7 },
        { x: 3, y: 9 },
        { x: 4, y: 11 },
        { x: 5, y: 13 },
      ];

      const pipeline = createPipeline(rows);
      const result = await computeLinearRegression(pipeline.batches(), "x", "y");

      expect(result.sampleSize).toBe(5);
      expect(result.slope).toBe(2);
      expect(result.intercept).toBe(3);
      expect(result.r).toBe(1);
      expect(result.r2).toBe(1);
      expect(result.formula).toContain("y = 2.0000 * x + 3.0000");
    });
  });

  describe("CLI Command Integration", () => {
    it("should execute rowpipe corr via CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} corr samples/titanic.csv --cols Age,Fare,SibSp,Parch --json`
      );
      const json = JSON.parse(stdout);
      expect(Array.isArray(json)).toBe(true);
      expect(json.length).toBe(4);
      expect(json[0]).toHaveProperty("column");
      expect(json[0]).toHaveProperty("Age");
      expect(json[0]).toHaveProperty("Fare");
    });

    it("should execute rowpipe quantiles via CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} quantiles samples/titanic.csv Fare --p 50,90,95 --json`
      );
      const json = JSON.parse(stdout);
      expect(Array.isArray(json)).toBe(true);
      const medianObj = json.find((item: any) => item.metric.includes("median"));
      expect(medianObj).toBeDefined();
    });

    it("should execute rowpipe outliers via CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} outliers samples/titanic.csv --col Fare --method zscore --threshold 3.5 --only-outliers --json`
      );
      const json = JSON.parse(stdout);
      expect(Array.isArray(json)).toBe(true);
      expect(json.length).toBeGreaterThan(0);
      // All filtered items should have very high Fare
      for (const row of json) {
        expect(Number(row.Fare)).toBeGreaterThan(100);
      }
    });

    it("should execute rowpipe crosstab via CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} crosstab samples/titanic.csv Pclass Survived --json`
      );
      const json = JSON.parse(stdout);
      expect(Array.isArray(json)).toBe(true);
      expect(json.length).toBeGreaterThanOrEqual(3);
    });

    it("should execute rowpipe regression via CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} regression samples/titanic.csv Age Fare --json`
      );
      const json = JSON.parse(stdout);
      expect(Array.isArray(json)).toBe(true);
      const formulaObj = json.find((item: any) => item.metric === "formula");
      expect(formulaObj).toBeDefined();
    });
  });
});
