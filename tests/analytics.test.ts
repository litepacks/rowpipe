import { describe, it, expect } from "vitest";
import { NumericStatsCollector, HyperLogLog, DatasetStatsAggregator } from "../src/analytics/stats.js";
import { SchemaInferenceAggregator } from "../src/analytics/schema-inference.js";
import { detectSemanticType } from "../src/analytics/semantic-types.js";
import { SchemaValidatorAggregator } from "../src/analytics/validator.js";
import type { Row } from "../src/core/types.js";

describe("Welford Numeric Statistics", () => {
  it("should calculate exact mean, variance, stddev, sum, min, max", () => {
    const collector = new NumericStatsCollector();
    const values = [10, 20, 30, 40, 50]; // mean = 30, variance = 250, stddev = 15.811...

    for (const val of values) {
      collector.add(val);
    }

    const res = collector.result();
    expect(res.count).toBe(5);
    expect(res.nullCount).toBe(0);
    expect(res.min).toBe(10);
    expect(res.max).toBe(50);
    expect(res.sum).toBe(150);
    expect(res.mean).toBe(30);
    expect(res.variance).toBe(250);
    expect(res.stddev).toBeCloseTo(15.811388, 4);
  });

  it("should handle null and missing values properly", () => {
    const collector = new NumericStatsCollector();
    collector.add(10);
    collector.add(null);
    collector.add("");
    collector.add(30);

    const res = collector.result();
    expect(res.count).toBe(2);
    expect(res.nullCount).toBe(2);
    expect(res.mean).toBe(20);
  });
});

describe("HyperLogLog Distinct Estimator", () => {
  it("should estimate cardinality accurately with bounded memory", () => {
    const hll = new HyperLogLog(10);
    const uniqueCount = 5000;

    for (let i = 0; i < uniqueCount; i++) {
      hll.add(`user_${i}`);
    }

    const estimated = hll.count();
    // With p=10, error is typically within ~5%
    const errorRatio = Math.abs(estimated - uniqueCount) / uniqueCount;
    expect(errorRatio).toBeLessThan(0.08);
  });
});

describe("Schema Inference", () => {
  it("should infer types, nullability, and confidence", () => {
    const rows: Row[] = [
      { id: "1", name: "Alice", email: "alice@test.com", age: "30", created_at: "2026-01-01" },
      { id: "2", name: "Bob", email: "bob@test.com", age: "25", created_at: "2026-01-02" },
      { id: "3", name: "Charlie", email: null, age: "40", created_at: "2026-01-03" },
    ];

    const agg = new SchemaInferenceAggregator();
    for (const row of rows) agg.add(row);

    const schema = agg.result();
    const colMap = new Map(schema.columns.map((c) => [c.name, c]));

    expect(colMap.get("id")?.type).toBe("integer");
    expect(colMap.get("id")?.nullable).toBe(false);

    expect(colMap.get("email")?.type).toBe("string");
    expect(colMap.get("email")?.nullable).toBe(true);
    expect(colMap.get("email")?.semantic).toBe("email");

    expect(colMap.get("age")?.type).toBe("integer");
    expect(colMap.get("created_at")?.type).toBe("date");
  });
});

describe("Semantic Type Detector", () => {
  it("should detect emails, urls, uuids, and ips", () => {
    expect(detectSemanticType("user@example.com")).toBe("email");
    expect(detectSemanticType("https://google.com/path")).toBe("url");
    expect(detectSemanticType("123e4567-e89b-12d3-a456-426614174000")).toBe("uuid");
    expect(detectSemanticType("192.168.1.1")).toBe("ipv4");
    expect(detectSemanticType("USD")).toBe("currency");
    expect(detectSemanticType("TR")).toBe("country-code");
  });
});

describe("Schema Validator", () => {
  it("should validate stream and report violations", () => {
    const schema = {
      id: { type: "integer", nullable: false },
      email: { type: "string", nullable: false, format: "email" },
      age: { type: "integer", nullable: true },
    };

    const rows: Row[] = [
      { id: 1, email: "valid@test.com", age: 30 },
      { id: 2, email: null, age: 25 }, // email null violation
      { id: "bad_id", email: "bad@test.com", age: "not_a_num" }, // id and age type violations
    ];

    const validator = new SchemaValidatorAggregator(schema);
    for (const row of rows) validator.add(row);

    const report = validator.result();
    expect(report.totalRows).toBe(3);
    expect(report.validRows).toBe(1);
    expect(report.invalidRows).toBe(2);
    expect(report.isValid).toBe(false);
    expect(report.violations.length).toBeGreaterThanOrEqual(3);
  });
});
