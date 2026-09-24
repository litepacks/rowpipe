import type { Row, DataBatch } from "../core/types.js";

export interface FunnelOptions {
  steps?: string | string[];
  userIdCol?: string;
  stepCol?: string;
  timeCol?: string;
  strict?: boolean;
  windowMs?: number;
}

export interface FunnelStepMetric {
  step: string;
  step_index: number;
  users: number;
  step_conversion_pct: number;
  overall_conversion_pct: number;
  dropoff_users: number;
  dropoff_pct: number;
  visual_bar: string;
}

export interface FunnelResult {
  steps: FunnelStepMetric[];
  total_entry_users: number;
  final_converted_users: number;
  overall_conversion_rate: number;
  formattedRows: Row[];
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

function renderBar(pct: number, width = 20): string {
  const filled = Math.max(0, Math.min(width, Math.round((pct / 100) * width)));
  const empty = width - filled;
  return "█".repeat(filled) + "░".repeat(empty);
}

export function parseStepList(stepInput?: string | string[]): string[] {
  if (!stepInput) return [];
  if (Array.isArray(stepInput)) {
    return stepInput
      .flatMap((s) => s.split(/->|,|>|\|/))
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return stepInput
    .split(/->|,|>|\|/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export async function computeFunnel(
  batchIterator: AsyncIterable<DataBatch>,
  options: FunnelOptions = {}
): Promise<FunnelResult> {
  const steps = parseStepList(options.steps);
  if (steps.length === 0) {
    throw new Error(
      "At least two funnel steps are required (e.g. --steps 'view,cart,checkout,purchase' or 'view->cart->checkout')"
    );
  }

  const stepIndexMap = new Map<string, number>();
  for (let i = 0; i < steps.length; i++) {
    stepIndexMap.set(steps[i]!.toLowerCase(), i);
  }

  let userIdCol = options.userIdCol;
  let stepCol = options.stepCol;
  let timeCol = options.timeCol;
  const isStrict = options.strict ?? false;

  // Map: userId -> Array of events { stepIndex: number, timeMs: number }
  const userEvents = new Map<string, Array<{ stepIndex: number; timeMs: number }>>();

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!userIdCol || !stepCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);

      if (!userIdCol) {
        userIdCol =
          keys.find((k) => /^(user(_?id)?|customer(_?id)?|client(_?id)?|account(_?id)?|uid|id)$/i.test(k)) ??
          keys[0];
      }
      if (!stepCol) {
        stepCol =
          keys.find((k) => /(step|event(_?name)?|action|page|stage|status)/i.test(k)) ??
          keys[1];
      }
      if (!timeCol) {
        timeCol = keys.find((k) => /(date|time|timestamp|created_at|occurred_at)/i.test(k));
      }
    }

    for (const row of batch.rows) {
      const rawUid = row[userIdCol!];
      if (rawUid === null || rawUid === undefined || rawUid === "") continue;
      const uid = String(rawUid);

      const rawStep = row[stepCol!];
      if (rawStep === null || rawStep === undefined || rawStep === "") continue;
      const stepStr = String(rawStep).toLowerCase().trim();

      const stepIdx = stepIndexMap.get(stepStr);
      if (stepIdx === undefined) continue;

      let tMs = 0;
      if (timeCol) {
        tMs = parseDateToMs(row[timeCol]) ?? 0;
      }

      let events = userEvents.get(uid);
      if (!events) {
        events = [];
        userEvents.set(uid, events);
      }
      events.push({ stepIndex: stepIdx, timeMs: tMs });
    }
  }

  // Count how many users completed each stage in the funnel
  const completedStepCounts = new Array<number>(steps.length).fill(0);

  for (const events of userEvents.values()) {
    if (events.length === 0) continue;

    if (timeCol || isStrict) {
      events.sort((a, b) => a.timeMs - b.timeMs);
    }

    let currentStepTarget = 0;
    let firstStepTime = 0;

    for (const ev of events) {
      if (ev.stepIndex === currentStepTarget) {
        if (currentStepTarget === 0) {
          firstStepTime = ev.timeMs;
        }

        if (options.windowMs && firstStepTime > 0 && ev.timeMs - firstStepTime > options.windowMs) {
          // Out of conversion window
          break;
        }

        completedStepCounts[currentStepTarget] = (completedStepCounts[currentStepTarget] || 0) + 1;
        currentStepTarget += 1;

        if (currentStepTarget >= steps.length) {
          break;
        }
      }
    }
  }

  const baseUsers = completedStepCounts[0] || 0;
  const metrics: FunnelStepMetric[] = [];
  const formattedRows: Row[] = [];

  for (let i = 0; i < steps.length; i++) {
    const stepName = steps[i]!;
    const users = completedStepCounts[i] || 0;
    const prevUsers = i === 0 ? users : completedStepCounts[i - 1] || 0;

    const stepConv = prevUsers > 0 ? (users / prevUsers) * 100 : 0;
    const overallConv = baseUsers > 0 ? (users / baseUsers) * 100 : 0;
    const dropoff = i === 0 ? 0 : Math.max(0, prevUsers - users);
    const dropoffPct = i === 0 ? 0 : 100 - stepConv;

    const metric: FunnelStepMetric = {
      step: stepName,
      step_index: i + 1,
      users,
      step_conversion_pct: Math.round(stepConv * 10) / 10,
      overall_conversion_pct: Math.round(overallConv * 10) / 10,
      dropoff_users: dropoff,
      dropoff_pct: Math.round(dropoffPct * 10) / 10,
      visual_bar: `${renderBar(overallConv, 15)} ${(Math.round(overallConv * 10) / 10).toFixed(1)}%`,
    };
    metrics.push(metric);

    formattedRows.push({
      step: stepName,
      users,
      step_conv: i === 0 ? "100.0%" : `${metric.step_conversion_pct.toFixed(1)}%`,
      overall_conv: `${metric.overall_conversion_pct.toFixed(1)}%`,
      dropoff: i === 0 ? "-" : `${dropoff} (${metric.dropoff_pct.toFixed(1)}%)`,
      funnel: metric.visual_bar,
    });
  }

  const finalUsers = completedStepCounts[steps.length - 1] || 0;
  const totalRate = baseUsers > 0 ? (finalUsers / baseUsers) * 100 : 0;

  return {
    steps: metrics,
    total_entry_users: baseUsers,
    final_converted_users: finalUsers,
    overall_conversion_rate: Math.round(totalRate * 10) / 10,
    formattedRows,
  };
}
