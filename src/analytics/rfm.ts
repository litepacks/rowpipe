import type { Row, DataBatch } from "../core/types.js";

export interface RfmOptions {
  customerIdCol?: string;
  dateCol?: string;
  amountCol?: string;
  asOfDate?: string | Date;
}

export interface CustomerRfmRaw {
  customerId: string;
  lastDateMs: number;
  frequency: number;
  monetary: number;
}

export interface CustomerRfmResult {
  customer_id: string;
  recency_days: number;
  frequency: number;
  monetary: number;
  r_score: number;
  f_score: number;
  m_score: number;
  rfm_score: string;
  segment: string;
}

export interface RfmSegmentSummary {
  segment: string;
  customer_count: number;
  customer_percentage: number;
  total_revenue: number;
  revenue_percentage: number;
  avg_recency_days: number;
  avg_frequency: number;
  avg_monetary: number;
}

function parseDateToMs(val: unknown): number | null {
  if (val === null || val === undefined || val === "") return null;
  if (val instanceof Date && !isNaN(val.getTime())) return val.getTime();
  if (typeof val === "number" && Number.isFinite(val)) {
    return val < 100_000_000_000 ? Math.floor(val * 1000) : Math.floor(val);
  }
  if (typeof val === "string") {
    const trimmed = val.trim();
    if (!trimmed) return null;
    const num = Number(trimmed);
    if (!Number.isNaN(num) && /^\d{10,13}$/.test(trimmed)) {
      return num < 100_000_000_000 ? Math.floor(num * 1000) : Math.floor(num);
    }
    const parsed = Date.parse(trimmed);
    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }
  return null;
}

function assignQuintileScores(values: number[], reverse = false): number[] {
  if (values.length === 0) return [];
  const indexed = values.map((val, idx) => ({ val, idx }));
  indexed.sort((a, b) => a.val - b.val);

  const n = values.length;
  const scores = new Array<number>(n);

  for (let rank = 0; rank < n; rank++) {
    const origIdx = indexed[rank]!.idx;
    const pct = (rank + 1) / n;
    let score = 1;
    if (pct <= 0.2) score = 1;
    else if (pct <= 0.4) score = 2;
    else if (pct <= 0.6) score = 3;
    else if (pct <= 0.8) score = 4;
    else score = 5;

    if (reverse) {
      score = 6 - score;
    }
    scores[origIdx] = score;
  }
  return scores;
}

export function classifyRfmSegment(r: number, f: number, m: number): string {
  if (r >= 4 && f >= 4 && m >= 4) return "Champions";
  if (r <= 2 && f >= 4 && m >= 4) return "Can't Lose Them";
  if (r <= 2 && f >= 3 && m >= 3) return "At Risk";
  if (r >= 3 && f >= 3 && m >= 3) return "Loyal Customers";
  if (r >= 4 && f >= 2 && m >= 2) return "Potential Loyalists";
  if (r >= 4 && f <= 2) return "New Customers";
  if (r >= 3 && f <= 2 && m >= 2) return "Promising";
  if (r >= 2 && f >= 2 && m >= 2) return "Customers Needing Attention";
  if (r <= 2 && f <= 2 && m <= 2) return "Hibernating / Lost";
  if (r <= 2) return "About to Sleep";
  return "Promising";
}

export async function computeRfm(
  batchIterator: AsyncIterable<DataBatch>,
  options: RfmOptions = {}
): Promise<{
  customers: CustomerRfmResult[];
  summary: RfmSegmentSummary[];
  referenceDate: string;
}> {
  let customerIdCol = options.customerIdCol;
  let dateCol = options.dateCol;
  let amountCol = options.amountCol;

  const customerMap = new Map<string, CustomerRfmRaw>();
  let maxDateMs = 0;

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    // Auto-detect columns if not explicitly provided
    if (!customerIdCol || !dateCol || !amountCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);

      if (!customerIdCol) {
        customerIdCol =
          keys.find((k) => /^(customer(_?id)?|user(_?id)?|client(_?id)?|account(_?id)?|uid)$/i.test(k)) ??
          keys[0];
      }
      if (!dateCol) {
        dateCol =
          keys.find((k) => /(date|time|timestamp|created_at|order_date|trans_date)/i.test(k)) ??
          keys[1];
      }
      if (!amountCol) {
        amountCol =
          keys.find((k) => /(amount|total|price|monetary|revenue|cost|sales|spend|val)/i.test(k)) ??
          keys[2];
      }
    }

    for (const row of batch.rows) {
      const rawCid = row[customerIdCol!];
      if (rawCid === null || rawCid === undefined || rawCid === "") continue;
      const cid = String(rawCid);

      const dateMs = parseDateToMs(row[dateCol!]);
      const rawAmt = row[amountCol!];
      const amount = typeof rawAmt === "number" ? rawAmt : parseFloat(String(rawAmt)) || 0;

      let cust = customerMap.get(cid);
      if (!cust) {
        cust = {
          customerId: cid,
          lastDateMs: dateMs ?? 0,
          frequency: 0,
          monetary: 0,
        };
        customerMap.set(cid, cust);
      }

      cust.frequency += 1;
      cust.monetary += amount;
      if (dateMs !== null && dateMs > cust.lastDateMs) {
        cust.lastDateMs = dateMs;
      }
      if (dateMs !== null && dateMs > maxDateMs) {
        maxDateMs = dateMs;
      }
    }
  }

  // Determine reference date (as-of date)
  let refMs = maxDateMs;
  if (options.asOfDate) {
    const userRef = parseDateToMs(options.asOfDate);
    if (userRef) refMs = userRef;
  }
  if (refMs === 0) {
    refMs = Date.now();
  }

  const customerList = Array.from(customerMap.values());
  if (customerList.length === 0) {
    return {
      customers: [],
      summary: [],
      referenceDate: new Date(refMs).toISOString(),
    };
  }

  const recencyValues: number[] = [];
  const freqValues: number[] = [];
  const monValues: number[] = [];

  for (const c of customerList) {
    const days = c.lastDateMs > 0 ? Math.max(0, Math.floor((refMs - c.lastDateMs) / (1000 * 60 * 60 * 24))) : 999;
    recencyValues.push(days);
    freqValues.push(c.frequency);
    monValues.push(c.monetary);
  }

  // Recency: lower days is better -> reverse = true
  const rScores = assignQuintileScores(recencyValues, true);
  // Frequency: higher count is better -> reverse = false
  const fScores = assignQuintileScores(freqValues, false);
  // Monetary: higher amount is better -> reverse = false
  const mScores = assignQuintileScores(monValues, false);

  const results: CustomerRfmResult[] = [];
  let totalRevenueAll = 0;

  for (let i = 0; i < customerList.length; i++) {
    const c = customerList[i]!;
    const r = rScores[i]!;
    const f = fScores[i]!;
    const m = mScores[i]!;
    const seg = classifyRfmSegment(r, f, m);
    totalRevenueAll += c.monetary;

    results.push({
      customer_id: c.customerId,
      recency_days: recencyValues[i]!,
      frequency: c.frequency,
      monetary: Math.round(c.monetary * 100) / 100,
      r_score: r,
      f_score: f,
      m_score: m,
      rfm_score: `${r}${f}${m}`,
      segment: seg,
    });
  }

  // Calculate Segment Summary
  const segmentStats = new Map<
    string,
    { count: number; totalRev: number; totalRecency: number; totalFreq: number; totalMon: number }
  >();

  for (const item of results) {
    let stat = segmentStats.get(item.segment);
    if (!stat) {
      stat = { count: 0, totalRev: 0, totalRecency: 0, totalFreq: 0, totalMon: 0 };
      segmentStats.set(item.segment, stat);
    }
    stat.count += 1;
    stat.totalRev += item.monetary;
    stat.totalRecency += item.recency_days;
    stat.totalFreq += item.frequency;
    stat.totalMon += item.monetary;
  }

  const summaryList: RfmSegmentSummary[] = [];
  const totalCusts = results.length;

  for (const [seg, st] of segmentStats.entries()) {
    summaryList.push({
      segment: seg,
      customer_count: st.count,
      customer_percentage: Math.round((st.count / totalCusts) * 1000) / 10,
      total_revenue: Math.round(st.totalRev * 100) / 100,
      revenue_percentage: totalRevenueAll > 0 ? Math.round((st.totalRev / totalRevenueAll) * 1000) / 10 : 0,
      avg_recency_days: Math.round((st.totalRecency / st.count) * 10) / 10,
      avg_frequency: Math.round((st.totalFreq / st.count) * 10) / 10,
      avg_monetary: Math.round((st.totalMon / st.count) * 100) / 100,
    });
  }

  summaryList.sort((a, b) => b.total_revenue - a.total_revenue);

  return {
    customers: results,
    summary: summaryList,
    referenceDate: new Date(refMs).toISOString().split("T")[0]!,
  };
}
