import type { Row, DataBatch } from "../core/types.js";

export type CohortInterval = "1d" | "1w" | "1mo" | "1y" | "day" | "week" | "month" | "year";

export interface CohortOptions {
  userIdCol?: string;
  timeCol?: string;
  interval?: CohortInterval;
  percent?: boolean;
  maxPeriods?: number;
}

export interface CohortMatrixResult {
  cohorts: Array<{
    cohort: string;
    total_users: number;
    periods: number[];
    retention_rates: number[];
  }>;
  formattedRows: Row[];
  interval: string;
  maxPeriodIndex: number;
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

function getPeriodKeyAndIndex(ms: number, interval: string): { key: string; index: number } {
  const d = new Date(ms);
  const year = d.getUTCFullYear();
  const month = d.getUTCMonth(); // 0-11
  const date = d.getUTCDate();

  const normInterval = interval.toLowerCase();

  if (normInterval === "1d" || normInterval === "day") {
    const key = `${year}-${String(month + 1).padStart(2, "0")}-${String(date).padStart(2, "0")}`;
    const dayIndex = Math.floor(ms / (1000 * 60 * 60 * 24));
    return { key, index: dayIndex };
  }

  if (normInterval === "1w" || normInterval === "week") {
    // Week index from epoch
    const weekIndex = Math.floor(ms / (1000 * 60 * 60 * 24 * 7));
    // Start of week date
    const sowMs = weekIndex * 7 * 24 * 60 * 60 * 1000;
    const sow = new Date(sowMs);
    const key = `W${year}-${String(sow.getUTCMonth() + 1).padStart(2, "0")}-${String(sow.getUTCDate()).padStart(2, "0")}`;
    return { key, index: weekIndex };
  }

  if (normInterval === "1y" || normInterval === "year") {
    return { key: `${year}`, index: year };
  }

  // Default: 1mo / month
  const key = `${year}-${String(month + 1).padStart(2, "0")}`;
  const monthIndex = year * 12 + month;
  return { key, index: monthIndex };
}

export async function computeCohort(
  batchIterator: AsyncIterable<DataBatch>,
  options: CohortOptions = {}
): Promise<CohortMatrixResult> {
  const interval = options.interval || "1mo";
  const maxPeriods = options.maxPeriods ?? 12;
  const isPercent = options.percent ?? true;

  let userIdCol = options.userIdCol;
  let timeCol = options.timeCol;

  // Map: userId -> { minIndex: number, minKey: string, activeIndices: Set<number> }
  const userMap = new Map<string, { minIndex: number; minKey: string; activeIndices: Set<number> }>();

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!userIdCol || !timeCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);

      if (!userIdCol) {
        userIdCol =
          keys.find((k) => /^(user(_?id)?|customer(_?id)?|client(_?id)?|account(_?id)?|uid|id)$/i.test(k)) ??
          keys[0];
      }
      if (!timeCol) {
        timeCol =
          keys.find((k) => /(date|time|timestamp|created_at|event_time|occurred_at)/i.test(k)) ??
          keys[1];
      }
    }

    for (const row of batch.rows) {
      const rawUid = row[userIdCol!];
      if (rawUid === null || rawUid === undefined || rawUid === "") continue;
      const uid = String(rawUid);

      const ms = parseDateToMs(row[timeCol!]);
      if (ms === null) continue;

      const { key, index } = getPeriodKeyAndIndex(ms, interval);

      let uEntry = userMap.get(uid);
      if (!uEntry) {
        uEntry = { minIndex: index, minKey: key, activeIndices: new Set<number>() };
        userMap.set(uid, uEntry);
      } else if (index < uEntry.minIndex) {
        uEntry.minIndex = index;
        uEntry.minKey = key;
      }
      uEntry.activeIndices.add(index);
    }
  }

  // Aggregate cohorts
  // Map: cohortKey -> { minIndex: number, users: Set<string>, periodCounts: Map<offset, number> }
  const cohortGroups = new Map<
    string,
    { minIndex: number; userIds: Set<string>; periodCounts: Map<number, number> }
  >();

  let globalMaxOffset = 0;

  for (const [uid, uEntry] of userMap.entries()) {
    let group = cohortGroups.get(uEntry.minKey);
    if (!group) {
      group = {
        minIndex: uEntry.minIndex,
        userIds: new Set<string>(),
        periodCounts: new Map<number, number>(),
      };
      cohortGroups.set(uEntry.minKey, group);
    }
    group.userIds.add(uid);

    for (const actIdx of uEntry.activeIndices) {
      const offset = actIdx - uEntry.minIndex;
      if (offset >= 0 && offset <= maxPeriods) {
        group.periodCounts.set(offset, (group.periodCounts.get(offset) || 0) + 1);
        if (offset > globalMaxOffset) {
          globalMaxOffset = offset;
        }
      }
    }
  }

  // Sort cohorts chronologically
  const sortedCohorts = Array.from(cohortGroups.entries()).sort((a, b) => a[1].minIndex - b[1].minIndex);

  const cohortResults: CohortMatrixResult["cohorts"] = [];
  const formattedRows: Row[] = [];

  const periodLabelPrefix =
    interval.startsWith("1d") || interval === "day"
      ? "+Day "
      : interval.startsWith("1w") || interval === "week"
        ? "+Wk "
        : interval.startsWith("1y") || interval === "year"
          ? "+Yr "
          : "+M";

  for (const [cKey, g] of sortedCohorts) {
    const totalUsers = g.userIds.size;
    const periods: number[] = [];
    const retentionRates: number[] = [];

    const rowObj: Row = {
      cohort: cKey,
      users: totalUsers,
    };

    for (let p = 0; p <= globalMaxOffset; p++) {
      const count = g.periodCounts.get(p) || 0;
      const rate = totalUsers > 0 ? (count / totalUsers) * 100 : 0;
      periods.push(count);
      retentionRates.push(Math.round(rate * 10) / 10);

      const colName = `${periodLabelPrefix}${p}`;
      if (isPercent) {
        rowObj[colName] = count > 0 ? `${(Math.round(rate * 10) / 10).toFixed(1)}%` : "-";
      } else {
        rowObj[colName] = count > 0 ? count : "-";
      }
    }

    cohortResults.push({
      cohort: cKey,
      total_users: totalUsers,
      periods,
      retention_rates: retentionRates,
    });

    formattedRows.push(rowObj);
  }

  return {
    cohorts: cohortResults,
    formattedRows,
    interval,
    maxPeriodIndex: globalMaxOffset,
  };
}
