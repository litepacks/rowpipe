import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { computeRfm } from "../src/analytics/rfm.js";
import { computeCohort } from "../src/analytics/cohort.js";
import { computeFunnel } from "../src/analytics/funnel.js";
import { computeAbTest } from "../src/analytics/abtest.js";
import { computePareto } from "../src/analytics/pareto.js";
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

describe("Business & Growth BI Suite (RFM, Cohort, Funnel, ABTest, Pareto)", () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "rowpipe-bi-test-"));
  });

  afterEach(async () => {
    await fs.rm(tmpDir, { recursive: true, force: true });
  });

  describe("computeRfm (Customer Lifecycle Segmentation)", () => {
    it("should calculate recency, frequency, monetary, and segments", async () => {
      const rows = [
        { customer_id: "c1", date: "2024-01-01", amount: 100 },
        { customer_id: "c1", date: "2024-01-10", amount: 200 },
        { customer_id: "c1", date: "2024-01-15", amount: 300 },
        { customer_id: "c2", date: "2023-01-01", amount: 50 },
        { customer_id: "c3", date: "2024-01-14", amount: 500 },
      ];

      const res = await computeRfm(createBatchIterator(rows), {
        customerIdCol: "customer_id",
        dateCol: "date",
        amountCol: "amount",
        asOfDate: "2024-01-16",
      });

      expect(res.customers.length).toBe(3);
      const c1 = res.customers.find((c) => c.customer_id === "c1");
      expect(c1).toBeDefined();
      expect(c1!.frequency).toBe(3);
      expect(c1!.monetary).toBe(600);
      expect(c1!.recency_days).toBe(1); // 2024-01-16 - 2024-01-15 = 1 day
      expect(c1!.r_score).toBeGreaterThanOrEqual(4);

      const c2 = res.customers.find((c) => c.customer_id === "c2");
      expect(c2).toBeDefined();
      expect(c2!.recency_days).toBeGreaterThan(300);

      expect(res.summary.length).toBeGreaterThan(0);
    });
  });

  describe("computeCohort (User Retention Matrix)", () => {
    it("should calculate monthly cohort retention matrix", async () => {
      const rows = [
        // User 1 joined 2024-01, active in 2024-01 and 2024-02
        { user_id: "u1", date: "2024-01-05" },
        { user_id: "u1", date: "2024-02-10" },
        // User 2 joined 2024-01, active only in 2024-01
        { user_id: "u2", date: "2024-01-15" },
        // User 3 joined 2024-02, active in 2024-02
        { user_id: "u3", date: "2024-02-01" },
      ];

      const res = await computeCohort(createBatchIterator(rows), {
        userIdCol: "user_id",
        timeCol: "date",
        interval: "1mo",
      });

      expect(res.cohorts.length).toBe(2);
      const janCohort = res.cohorts.find((c) => c.cohort === "2024-01");
      expect(janCohort).toBeDefined();
      expect(janCohort!.total_users).toBe(2);
      expect(janCohort!.periods[0]).toBe(2); // M0
      expect(janCohort!.periods[1]).toBe(1); // M1 (user 1 returned)
      expect(janCohort!.retention_rates[0]).toBe(100);
      expect(janCohort!.retention_rates[1]).toBe(50);
    });
  });

  describe("computeFunnel (Conversion & Drop-off)", () => {
    it("should calculate sequential conversion and drop-off rates", async () => {
      const rows = [
        { user_id: "u1", step: "view" },
        { user_id: "u1", step: "cart" },
        { user_id: "u1", step: "purchase" },
        { user_id: "u2", step: "view" },
        { user_id: "u2", step: "cart" },
        { user_id: "u3", step: "view" },
      ];

      const res = await computeFunnel(createBatchIterator(rows), {
        steps: "view,cart,purchase",
        userIdCol: "user_id",
        stepCol: "step",
      });

      expect(res.steps.length).toBe(3);
      expect(res.steps[0]!.users).toBe(3); // view
      expect(res.steps[0]!.step_conversion_pct).toBe(100);
      expect(res.steps[1]!.users).toBe(2); // cart
      expect(res.steps[1]!.dropoff_users).toBe(1);
      expect(res.steps[2]!.users).toBe(1); // purchase
      expect(res.overall_conversion_rate).toBeCloseTo(33.3, 0);
    });
  });

  describe("computeAbTest (Statistical Significance)", () => {
    it("should perform Two-Proportion Z-Test for binary conversions", async () => {
      // Group A: 10% conversion (100 / 1000)
      // Group B: 15% conversion (150 / 1000)
      const rows: any[] = [];
      for (let i = 0; i < 1000; i++) {
        rows.push({ variant: "control", converted: i < 100 ? 1 : 0 });
      }
      for (let i = 0; i < 1000; i++) {
        rows.push({ variant: "treatment", converted: i < 150 ? 1 : 0 });
      }

      const res = await computeAbTest(createBatchIterator(rows), {
        groupCol: "variant",
        metricCol: "converted",
        controlGroup: "control",
        variantGroup: "treatment",
      });

      expect(res.test_type).toBe("Two-Proportion Z-Test");
      expect(res.control_sample_size).toBe(1000);
      expect(res.variant_sample_size).toBe(1000);
      expect(res.control_metric).toBeCloseTo(0.1, 2);
      expect(res.variant_metric).toBeCloseTo(0.15, 2);
      expect(res.relative_lift_pct).toBeCloseTo(50, 0);
      expect(res.test_statistic).toBeGreaterThan(3.0); // Z ~ 3.3
      expect(res.p_value).toBeLessThan(0.01);
      expect(res.significant).toBe(true);
      expect(res.verdict).toContain("Statistically Significant Winner");
    });

    it("should perform Welch's T-Test for continuous values", async () => {
      const rows: any[] = [];
      for (let i = 0; i < 100; i++) {
        rows.push({ group: "control", revenue: 50 + (i % 10) });
        rows.push({ group: "variant", revenue: 80 + (i % 10) });
      }

      const res = await computeAbTest(createBatchIterator(rows), {
        groupCol: "group",
        metricCol: "revenue",
        controlGroup: "control",
        variantGroup: "variant",
        type: "means",
      });

      expect(res.test_type).toBe("Welch's Two-Sample T-Test");
      expect(res.control_metric).toBeCloseTo(54.5, 1);
      expect(res.variant_metric).toBeCloseTo(84.5, 1);
      expect(res.test_statistic).toBeGreaterThan(10);
      expect(res.p_value).toBeLessThan(0.001);
      expect(res.significant).toBe(true);
    });
  });

  describe("computePareto (80/20 & ABC Inventory Analysis)", () => {
    it("should assign ABC classes and calculate Pareto 80/20 distribution", async () => {
      const rows = [
        { product: "P1", revenue: 1000 },
        { product: "P2", revenue: 500 },
        { product: "P3", revenue: 300 },
        { product: "P4", revenue: 100 },
        { product: "P5", revenue: 50 },
        { product: "P6", revenue: 50 },
      ]; // Total: 2000

      const res = await computePareto(createBatchIterator(rows), {
        itemCol: "product",
        valueCol: "revenue",
      });

      expect(res.total_items).toBe(6);
      expect(res.total_value).toBe(2000);
      expect(res.items[0]!.item).toBe("P1");
      expect(res.items[0]!.abc_class).toBe("A"); // 1000 / 2000 = 50%
      expect(res.items[1]!.item).toBe("P2");
      expect(res.items[1]!.abc_class).toBe("A"); // 1500 / 2000 = 75%
      expect(res.summary.length).toBe(3); // Class A, B, C
    });
  });

  describe("CLI Command Integration", () => {
    it("should execute rowpipe rfm CLI", async () => {
      const csvPath = path.join(tmpDir, "orders.csv");
      await fs.writeFile(
        csvPath,
        "customer_id,date,amount\nC1,2024-01-01,100\nC1,2024-01-10,200\nC2,2023-01-01,50\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} rfm ${csvPath} --json`);
      const parsed = JSON.parse(stdout);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(2);
    });

    it("should execute rowpipe cohort CLI", async () => {
      const csvPath = path.join(tmpDir, "events.csv");
      await fs.writeFile(
        csvPath,
        "user_id,date\nu1,2024-01-05\nu1,2024-02-10\nu2,2024-01-15\nu3,2024-02-01\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} cohort ${csvPath} --json`);
      const parsed = JSON.parse(stdout);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(2);
    });

    it("should execute rowpipe funnel CLI", async () => {
      const csvPath = path.join(tmpDir, "funnel.csv");
      await fs.writeFile(
        csvPath,
        "user_id,step\nu1,view\nu1,cart\nu1,buy\nu2,view\nu2,cart\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} funnel ${csvPath} --steps "view->cart->buy" --json`);
      const parsed = JSON.parse(stdout);
      expect(parsed.steps).toBeDefined();
      expect(parsed.steps.length).toBe(3);
    });

    it("should execute rowpipe abtest CLI", async () => {
      const csvPath = path.join(tmpDir, "ab.csv");
      await fs.writeFile(
        csvPath,
        "group,converted\ncontrol,0\ncontrol,1\nvariant,1\nvariant,1\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} abtest ${csvPath} --json`);
      const parsed = JSON.parse(stdout);
      expect(parsed.test_type).toBeDefined();
      expect(parsed.control_sample_size).toBe(2);
      expect(parsed.variant_sample_size).toBe(2);
    });

    it("should execute rowpipe pareto CLI", async () => {
      const csvPath = path.join(tmpDir, "sales.csv");
      await fs.writeFile(
        csvPath,
        "item,revenue\nLaptop,1000\nPhone,500\nCable,50\n"
      );

      const { stdout } = await execAsync(`node ${CLI_PATH} pareto ${csvPath} --summary --json`);
      const parsed = JSON.parse(stdout);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(3);
    });
  });
});
