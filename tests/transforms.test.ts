import { describe, it, expect } from "vitest";
import { selectColumns } from "../src/transforms/select.js";
import { renameColumns, parseRenameSpecs } from "../src/transforms/rename.js";
import { castColumns, parseCastSpecs } from "../src/transforms/cast.js";
import { sampleRows } from "../src/transforms/sample.js";
import { filterRows } from "../src/transforms/filter.js";
import { compileExpression } from "../src/transforms/expression.js";
import { rowsToBatches } from "../src/core/batch.js";
import type { DataBatch, Row } from "../src/core/types.js";

async function collectRows(stream: AsyncIterable<DataBatch>): Promise<Row[]> {
  const results: Row[] = [];
  for await (const batch of stream) {
    results.push(...batch.rows);
  }
  return results;
}

describe("Select Transform", () => {
  it("should project selected columns", async () => {
    const rows: Row[] = [
      { id: 1, name: "Alice", email: "alice@example.com", age: 30 },
      { id: 2, name: "Bob", email: "bob@example.com", age: 25 },
    ];

    const stream = selectColumns(["id", "email"])(rowsToBatches(rows));
    const results = await collectRows(stream);

    expect(results).toEqual([
      { id: 1, email: "alice@example.com" },
      { id: 2, email: "bob@example.com" },
    ]);
  });
});

describe("Rename Transform", () => {
  it("should rename columns and parse rename specs", async () => {
    const specs = ["username=name", "user_age=age"];
    const mapping = parseRenameSpecs(specs);
    expect(mapping).toEqual({ username: "name", user_age: "age" });

    const rows: Row[] = [
      { username: "alice", user_age: 30, country: "TR" },
    ];

    const stream = renameColumns(mapping)(rowsToBatches(rows));
    const results = await collectRows(stream);

    expect(results).toEqual([
      { name: "alice", age: 30, country: "TR" },
    ]);
  });
});

describe("Cast Transform", () => {
  it("should cast columns according to specs", async () => {
    const specs = parseCastSpecs(["age:integer", "revenue:number", "active:boolean"]);
    expect(specs).toEqual({
      age: "integer",
      revenue: "number",
      active: "boolean",
    });

    const rows: Row[] = [
      { id: "1", age: "32", revenue: "1250.50", active: "true" },
      { id: "2", age: "invalid", revenue: "200", active: "0" },
    ];

    const stream = castColumns(specs, { onError: "null" })(rowsToBatches(rows));
    const results = await collectRows(stream);

    expect(results).toEqual([
      { id: "1", age: 32, revenue: 1250.5, active: true },
      { id: "2", age: null, revenue: 200, active: false },
    ]);
  });

  it("should support skip-row error handling", async () => {
    const specs = { age: "integer" };
    const rows: Row[] = [
      { id: "1", age: "30" },
      { id: "2", age: "invalid" },
      { id: "3", age: "40" },
    ];

    const stream = castColumns(specs, { onError: "skip-row" })(rowsToBatches(rows));
    const results = await collectRows(stream);

    expect(results).toEqual([
      { id: "1", age: 30 },
      { id: "3", age: 40 },
    ]);
  });
});

describe("Sample Transform", () => {
  it("should sample k rows deterministically with seed", async () => {
    const rows: Row[] = [];
    for (let i = 0; i < 1000; i++) {
      rows.push({ id: i, val: `row_${i}` });
    }

    const stream1 = sampleRows({ rows: 10, seed: 42 })(rowsToBatches(rows));
    const sample1 = await collectRows(stream1);

    const stream2 = sampleRows({ rows: 10, seed: 42 })(rowsToBatches(rows));
    const sample2 = await collectRows(stream2);

    expect(sample1.length).toBe(10);
    expect(sample1).toEqual(sample2);
  });
});

describe("Filter Transform & Expression Parser", () => {
  it("should evaluate comparisons and logical operators", () => {
    const fn1 = compileExpression('age > 30 && country == "TR"');
    expect(fn1({ age: 35, country: "TR" })).toBe(true);
    expect(fn1({ age: 25, country: "TR" })).toBe(false);
    expect(fn1({ age: 35, country: "US" })).toBe(false);

    const fn2 = compileExpression('revenue >= 1000 || is_vip == true');
    expect(fn2({ revenue: 500, is_vip: true })).toBe(true);
    expect(fn2({ revenue: 1200, is_vip: false })).toBe(true);
    expect(fn2({ revenue: 800, is_vip: false })).toBe(false);
  });

  it("should evaluate standard and pipe-syntax functions", () => {
    // 1. Pipe syntax for string transforms: email | lower | trim | endsWith(".com")
    const fn1 = compileExpression('email | lower | trim | endsWith(".com")');
    expect(fn1({ email: "  ADMIN@CORP.COM  " })).toBe(true);
    expect(fn1({ email: "user@corp.org" })).toBe(false);

    // 2. Date & Time functions with pipe: created_at | year == 2026
    const fn2 = compileExpression('created_at | year == 2026 && created_at | month >= 6');
    expect(fn2({ created_at: "2026-09-16" })).toBe(true);
    expect(fn2({ created_at: "2025-09-16" })).toBe(false);

    // 3. Set & Range functions: country | in("TR", "US", "DE") and age | between(18, 65)
    const fn3 = compileExpression('country | in("TR", "US", "DE") && age | between(18, 65)');
    expect(fn3({ country: "TR", age: 30 })).toBe(true);
    expect(fn3({ country: "FR", age: 30 })).toBe(false);
    expect(fn3({ country: "US", age: 70 })).toBe(false);

    // 4. String splitIndex and substring with pipe: email | splitIndex("@", 1) == "gmail.com"
    const fn4 = compileExpression('email | splitIndex("@", 1) == "gmail.com"');
    expect(fn4({ email: "john.doe@gmail.com" })).toBe(true);
    expect(fn4({ email: "john.doe@yahoo.com" })).toBe(false);

    // 5. Type inspection: email | isEmail
    const fn5 = compileExpression('email | isEmail');
    expect(fn5({ email: "test@example.com" })).toBe(true);
    expect(fn5({ email: "not-an-email" })).toBe(false);

    // 6. JSON nested access: direct dot-notation & jsonGet fallback
    const fn6 = compileExpression('payload.user.city == "Istanbul"');
    expect(fn6({ payload: JSON.stringify({ user: { city: "Istanbul" } }) })).toBe(true);
    expect(fn6({ payload: JSON.stringify({ user: { city: "Ankara" } }) })).toBe(false);

    // Direct nested object access with array index and pipe
    const fn6b = compileExpression('payload.items.0.price > 50 && payload.user.email | lower | endsWith("@corp.com")');
    expect(
      fn6b({
        payload: {
          items: [{ price: 99.9 }],
          user: { email: "AHMET@CORP.COM" },
        },
      })
    ).toBe(true);
    expect(
      fn6b({
        payload: {
          items: [{ price: 20 }],
          user: { email: "AHMET@CORP.COM" },
        },
      })
    ).toBe(false);

    // 7. Math functions: price | clamp(10, 100)
    const fn7 = compileExpression('price | clamp(10, 100) == 50');
    expect(fn7({ price: 50 })).toBe(true);
    expect(fn7({ price: 200 })).toBe(false);
  });

  it("should filter stream using filterRows", async () => {
    const rows: Row[] = [
      { id: 1, age: 35, email: "alice@test.com" },
      { id: 2, age: 20, email: "bob@test.org" },
      { id: 3, age: 45, email: "charlie@test.com" },
    ];

    const stream = filterRows('age | between(30, 50) && email | endsWith(".com")')(rowsToBatches(rows));
    const results = await collectRows(stream);

    expect(results).toEqual([
      { id: 1, age: 35, email: "alice@test.com" },
      { id: 3, age: 45, email: "charlie@test.com" },
    ]);
  });
});
