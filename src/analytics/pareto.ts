import type { Row, DataBatch } from "../core/types.js";

export interface ParetoOptions {
  itemCol?: string;
  valueCol?: string;
  aThreshold?: number; // default: 80 (%)
  bThreshold?: number; // default: 95 (%)
  summary?: boolean;
}

export interface ParetoItemResult {
  rank: number;
  item: string;
  value: number;
  share_pct: number;
  cumulative_value: number;
  cumulative_pct: number;
  abc_class: "A" | "B" | "C";
}

export interface ParetoSummaryCategory {
  abc_class: "A" | "B" | "C";
  item_count: number;
  item_percentage: number;
  total_value: number;
  value_percentage: number;
  description: string;
}

export interface ParetoResult {
  items: ParetoItemResult[];
  summary: ParetoSummaryCategory[];
  total_items: number;
  total_value: number;
  pareto_80_item_count: number;
  pareto_80_item_percentage: number;
  formattedRows: Row[];
}

export async function computePareto(
  batchIterator: AsyncIterable<DataBatch>,
  options: ParetoOptions = {}
): Promise<ParetoResult> {
  let itemCol = options.itemCol;
  let valueCol = options.valueCol;
  const aThreshold = options.aThreshold ?? 80;
  const bThreshold = options.bThreshold ?? 95;

  const itemMap = new Map<string, number>();

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!itemCol || !valueCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);

      if (!itemCol) {
        itemCol =
          keys.find((k) => /^(item|product|sku|category|customer|name|title|key|id)$/i.test(k)) ??
          keys[0];
      }
      if (!valueCol) {
        valueCol =
          keys.find((k) => /(value|revenue|sales|amount|total|spend|count|qty|quantity|price)/i.test(k)) ??
          keys[1];
      }
    }

    for (const row of batch.rows) {
      const rawItem = row[itemCol!];
      if (rawItem === null || rawItem === undefined || rawItem === "") continue;
      const it = String(rawItem);

      const rawVal = row[valueCol!];
      let numVal = 0;
      if (typeof rawVal === "number") {
        numVal = rawVal;
      } else {
        const parsed = parseFloat(String(rawVal));
        if (!isNaN(parsed)) numVal = parsed;
      }

      itemMap.set(it, (itemMap.get(it) || 0) + numVal);
    }
  }

  const sortedList = Array.from(itemMap.entries()).sort((a, b) => b[1] - a[1]);
  const totalItems = sortedList.length;
  let totalValue = 0;
  for (const [, val] of sortedList) {
    totalValue += val;
  }

  const items: ParetoItemResult[] = [];
  let runningSum = 0;
  let pareto80Count = 0;

  for (let rank = 1; rank <= totalItems; rank++) {
    const [name, val] = sortedList[rank - 1]!;
    runningSum += val;
    const sharePct = totalValue > 0 ? (val / totalValue) * 100 : 0;
    const cumPct = totalValue > 0 ? (runningSum / totalValue) * 100 : 0;

    let abc: "A" | "B" | "C" = "C";
    if (cumPct <= aThreshold || (rank === 1 && cumPct > aThreshold)) {
      abc = "A";
    } else if (cumPct <= bThreshold) {
      abc = "B";
    } else {
      abc = "C";
    }

    if (cumPct <= 80 || (pareto80Count === 0 && cumPct > 80)) {
      pareto80Count = rank;
    }

    items.push({
      rank,
      item: name,
      value: Math.round(val * 100) / 100,
      share_pct: Math.round(sharePct * 100) / 100,
      cumulative_value: Math.round(runningSum * 100) / 100,
      cumulative_pct: Math.round(cumPct * 10) / 10,
      abc_class: abc,
    });
  }

  // Calculate Summary Categories
  const classA = items.filter((it) => it.abc_class === "A");
  const classB = items.filter((it) => it.abc_class === "B");
  const classC = items.filter((it) => it.abc_class === "C");

  const sumA = classA.reduce((acc, it) => acc + it.value, 0);
  const sumB = classB.reduce((acc, it) => acc + it.value, 0);
  const sumC = classC.reduce((acc, it) => acc + it.value, 0);

  const summary: ParetoSummaryCategory[] = [
    {
      abc_class: "A",
      item_count: classA.length,
      item_percentage: totalItems > 0 ? Math.round((classA.length / totalItems) * 1000) / 10 : 0,
      total_value: Math.round(sumA * 100) / 100,
      value_percentage: totalValue > 0 ? Math.round((sumA / totalValue) * 1000) / 10 : 0,
      description: "Vital Few: Top driving ~80% of value",
    },
    {
      abc_class: "B",
      item_count: classB.length,
      item_percentage: totalItems > 0 ? Math.round((classB.length / totalItems) * 1000) / 10 : 0,
      total_value: Math.round(sumB * 100) / 100,
      value_percentage: totalValue > 0 ? Math.round((sumB / totalValue) * 1000) / 10 : 0,
      description: "Moderate: Intermediate ~15% of value",
    },
    {
      abc_class: "C",
      item_count: classC.length,
      item_percentage: totalItems > 0 ? Math.round((classC.length / totalItems) * 1000) / 10 : 0,
      total_value: Math.round(sumC * 100) / 100,
      value_percentage: totalValue > 0 ? Math.round((sumC / totalValue) * 1000) / 10 : 0,
      description: "Trivial Many: Long tail ~5% of value",
    },
  ];

  const pareto80Pct = totalItems > 0 ? Math.round((pareto80Count / totalItems) * 1000) / 10 : 0;

  const formattedRows: Row[] = options.summary
    ? summary.map((s) => ({
        abc_class: s.abc_class,
        items: `${s.item_count} (${s.item_percentage.toFixed(1)}%)`,
        total_value: s.total_value,
        value_share: `${s.value_percentage.toFixed(1)}%`,
        description: s.description,
      }))
    : items.map((it) => ({
        rank: it.rank,
        item: it.item,
        value: it.value,
        share: `${it.share_pct.toFixed(2)}%`,
        cum_value: it.cumulative_value,
        cum_share: `${it.cumulative_pct.toFixed(1)}%`,
        class: it.abc_class,
      }));

  return {
    items,
    summary,
    total_items: totalItems,
    total_value: Math.round(totalValue * 100) / 100,
    pareto_80_item_count: pareto80Count,
    pareto_80_item_percentage: pareto80Pct,
    formattedRows,
  };
}
