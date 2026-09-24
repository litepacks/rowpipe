import { describe, it, expect } from "vitest";
import { rowsToBatches } from "../src/core/batch.js";
import type { DataBatch, Row } from "../src/core/types.js";
import { mapRows, parseMapSpecs } from "../src/transforms/map.js";
import { parseReduceSpecs, reduceRows, ReduceAggregator } from "../src/analytics/reduce.js";
import { filterRows } from "../src/transforms/filter.js";

async function collectRows(stream: AsyncIterable<DataBatch>): Promise<Row[]> {
  const results: Row[] = [];
  for await (const batch of stream) {
    results.push(...batch.rows);
  }
  return results;
}

describe("Map Transform (Row-level calculations & derivations)", () => {
  it("should parse map specifications correctly", () => {
    const specs = [
      "profit = revenue - cost",
      "tax=revenue * 0.20",
      "user_city = payload.user.city",
    ];
    const parsed = parseMapSpecs(specs);
    expect(parsed).toEqual({
      profit: "revenue - cost",
      tax: "revenue * 0.20",
      user_city: "payload.user.city",
    });
  });

  it("should compute mathematical and percentage formulas per row", async () => {
    const rows: Row[] = [
      { id: 1, revenue: 1000, cost: 600 },
      { id: 2, revenue: 2500, cost: 1500 },
    ];

    const stream = mapRows({
      profit: "revenue - cost",
      tax: "revenue * 0.20",
      margin: "((revenue - cost) / revenue) * 100",
    })(rowsToBatches(rows));

    const results = await collectRows(stream);
    expect(results).toEqual([
      { id: 1, revenue: 1000, cost: 600, profit: 400, tax: 200, margin: 40 },
      { id: 2, revenue: 2500, cost: 1500, profit: 1000, tax: 500, margin: 40 },
    ]);
  });

  it("should support JSON dot-notation, pipe syntax, and date extraction in map", async () => {
    const rows: Row[] = [
      {
        id: 1,
        created_at: "2026-09-16",
        email: "  ADMIN@ROWPIPE.IO ",
        payload: JSON.stringify({ user: { city: "Istanbul" }, items: [{ price: 99 }] }),
      },
    ];

    const stream = mapRows({
      city: "payload.user.city",
      first_item_price: "payload.items.0.price",
      clean_email: "email | lower | trim",
      year: "created_at | year",
    })(rowsToBatches(rows));

    const results = await collectRows(stream);
    expect(results[0]?.city).toBe("Istanbul");
    expect(results[0]?.first_item_price).toBe(99);
    expect(results[0]?.clean_email).toBe("admin@rowpipe.io");
    expect(results[0]?.year).toBe(2026);
  });

  it("should support custom function mapper in mapRows", async () => {
    const rows: Row[] = [{ a: 1 }, { a: 2 }];
    const stream = mapRows((row) => ({ ...row, doubled: (Number(row.a) || 0) * 2 }))(rowsToBatches(rows));
    const results = await collectRows(stream);
    expect(results).toEqual([
      { a: 1, doubled: 2 },
      { a: 2, doubled: 4 },
    ]);
  });
});

describe("Reduce Transform (Global and Group-By Aggregations)", () => {
  it("should parse reduce specifications correctly", () => {
    const specs = [
      "total_rev = sum(revenue)",
      "avg_margin = avg(margin)",
      "total_orders = count()",
      "unique_users = countDistinct(user_id)",
      "min_val = min(price)",
      "max_val = max(price)",
    ];
    const parsed = parseReduceSpecs(specs);
    expect(parsed).toEqual([
      { targetField: "total_rev", func: "sum", sourceExpr: "revenue" },
      { targetField: "avg_margin", func: "avg", sourceExpr: "margin" },
      { targetField: "total_orders", func: "count", sourceExpr: undefined },
      { targetField: "unique_users", func: "countdistinct", sourceExpr: "user_id" },
      { targetField: "min_val", func: "min", sourceExpr: "price" },
      { targetField: "max_val", func: "max", sourceExpr: "price" },
    ]);
  });

  it("should compute global aggregations across stream", async () => {
    const rows: Row[] = [
      { id: 1, user_id: "u1", revenue: 100, price: 50 },
      { id: 2, user_id: "u2", revenue: 200, price: 100 },
      { id: 3, user_id: "u1", revenue: 300, price: 150 },
      { id: 4, user_id: "u3", revenue: 400, price: 200 },
    ];

    const stream = reduceRows({
      aggregations: [
        "total_revenue = sum(revenue)",
        "avg_price = avg(price)",
        "min_price = min(price)",
        "max_price = max(price)",
        "order_count = count()",
        "unique_users = countDistinct(user_id)",
      ],
    })(rowsToBatches(rows));

    const results = await collectRows(stream);
    expect(results.length).toBe(1);
    expect(results[0]).toEqual({
      total_revenue: 1000,
      avg_price: 125,
      min_price: 50,
      max_price: 200,
      order_count: 4,
      unique_users: 3,
    });
  });

  it("should perform streaming group-by aggregations (--by country,category)", async () => {
    const rows: Row[] = [
      { country: "TR", category: "Electronics", revenue: 1000, cost: 600 },
      { country: "TR", category: "Electronics", revenue: 2000, cost: 1200 },
      { country: "TR", category: "Books", revenue: 300, cost: 100 },
      { country: "US", category: "Electronics", revenue: 5000, cost: 3000 },
      { country: "US", category: "Books", revenue: 800, cost: 400 },
    ];

    const stream = reduceRows({
      by: ["country", "category"],
      aggregations: [
        "total_revenue = sum(revenue)",
        "total_cost = sum(cost)",
        "item_count = count()",
      ],
    })(rowsToBatches(rows));

    const results = await collectRows(stream);
    expect(results.length).toBe(4);

    const trElectronics = results.find((r) => r.country === "TR" && r.category === "Electronics");
    expect(trElectronics).toEqual({
      country: "TR",
      category: "Electronics",
      total_revenue: 3000,
      total_cost: 1800,
      item_count: 2,
    });

    const usBooks = results.find((r) => r.country === "US" && r.category === "Books");
    expect(usBooks).toEqual({
      country: "US",
      category: "Books",
      total_revenue: 800,
      total_cost: 400,
      item_count: 1,
    });
  });

  it("should execute full Map -> Filter -> Reduce pipeline smoothly", async () => {
    const rows: Row[] = [
      { id: 1, country: "TR", revenue: 1000, cost: 600 },
      { id: 2, country: "TR", revenue: 500, cost: 450 },  // margin 10% (will be filtered out)
      { id: 3, country: "US", revenue: 3000, cost: 1500 }, // margin 50%
      { id: 4, country: "US", revenue: 2000, cost: 1600 }, // margin 20%
    ];

    // 1. Map: derive profit and margin
    const mapped = mapRows({
      profit: "revenue - cost",
      margin: "((revenue - cost) / revenue) * 100",
    })(rowsToBatches(rows));

    // 2. Filter: only margin >= 20
    const filtered = filterRows("margin >= 20")(mapped);

    // 3. Reduce: group by country
    const reduced = reduceRows({
      by: ["country"],
      aggregations: [
        "total_profit = sum(profit)",
        "avg_margin = avg(margin)",
        "count = count()",
      ],
    })(filtered);

    const results = await collectRows(reduced);
    expect(results.length).toBe(2);

    const tr = results.find((r) => r.country === "TR");
    expect(tr).toEqual({
      country: "TR",
      total_profit: 400,
      avg_margin: 40,
      count: 1,
    });

    const us = results.find((r) => r.country === "US");
    expect(us).toEqual({
      country: "US",
      total_profit: 1900,
      avg_margin: 35,
      count: 2,
    });
  });
});
