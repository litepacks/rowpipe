import type { DataBatch, DataStream, Row, TransformFunction } from "../core/types.js";
import { encodeCompositeKey } from "../diff/key.js";
import { InvalidArgumentError } from "../core/errors.js";

export interface WindowFunctionSpec {
  outputCol: string;
  fn:
    | "row_number"
    | "rank"
    | "dense_rank"
    | "lag"
    | "lead"
    | "running_sum"
    | "running_avg"
    | "running_count"
    | "running_min"
    | "running_max"
    | "moving_sum"
    | "moving_avg"
    | "moving_min"
    | "moving_max";
  sourceCol?: string;
  offset?: number;
  defaultValue?: unknown;
  windowSize?: number;
}

export interface WindowOptions {
  specs: Record<string, string> | WindowFunctionSpec[] | string[];
  by?: string | string[];
  batchSize?: number;
}

/**
 * Parses user window function specifications (e.g. "rn=row_number()", "lag(revenue, 1, 0)", "moving_avg(rev, 7)").
 */
export function parseWindowSpecs(input: Record<string, string> | WindowFunctionSpec[] | string[]): WindowFunctionSpec[] {
  if (Array.isArray(input)) {
    const parsed: WindowFunctionSpec[] = [];
    for (const item of input) {
      if (typeof item === "object") {
        parsed.push(item);
        continue;
      }

      const eqIdx = item.indexOf("=");
      if (eqIdx <= 0) {
        throw new InvalidArgumentError(`Invalid window spec: "${item}". Expected format: output_col=fn(...)`);
      }
      const outCol = item.slice(0, eqIdx).trim();
      const expr = item.slice(eqIdx + 1).trim();
      parsed.push(parseSingleWindowExpr(outCol, expr));
    }
    return parsed;
  }

  const parsed: WindowFunctionSpec[] = [];
  for (const [outCol, expr] of Object.entries(input)) {
    parsed.push(parseSingleWindowExpr(outCol, expr));
  }
  return parsed;
}

function parseSingleWindowExpr(outputCol: string, expr: string): WindowFunctionSpec {
  const match = /^([a-zA-Z0-9_]+)\s*\((.*)\)$/.exec(expr.trim());
  if (!match) {
    throw new InvalidArgumentError(`Invalid window function syntax: "${expr}"`);
  }

  const fnName = match[1]!.toLowerCase() as WindowFunctionSpec["fn"];
  const args = match[2]!
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);

  switch (fnName) {
    case "row_number":
    case "rank":
    case "dense_rank":
    case "running_count":
      return { outputCol, fn: fnName };

    case "lag": {
      const sourceCol = args[0];
      if (!sourceCol) throw new InvalidArgumentError(`lag() requires source column`);
      const offset = args[1] ? Number.parseInt(args[1], 10) : 1;
      const defaultValue = args[2] !== undefined ? JSON.parse(args[2]) : null;
      return { outputCol, fn: "lag", sourceCol, offset, defaultValue };
    }

    case "lead": {
      const sourceCol = args[0];
      if (!sourceCol) throw new InvalidArgumentError(`lead() requires source column`);
      const offset = args[1] ? Number.parseInt(args[1], 10) : 1;
      const defaultValue = args[2] !== undefined ? JSON.parse(args[2]) : null;
      return { outputCol, fn: "lead", sourceCol, offset, defaultValue };
    }

    case "running_sum":
    case "running_avg":
    case "running_min":
    case "running_max": {
      const sourceCol = args[0];
      if (!sourceCol) throw new InvalidArgumentError(`${fnName}() requires source column`);
      return { outputCol, fn: fnName, sourceCol };
    }

    case "moving_sum":
    case "moving_avg":
    case "moving_min":
    case "moving_max": {
      const sourceCol = args[0];
      if (!sourceCol) throw new InvalidArgumentError(`${fnName}() requires source column`);
      const windowSize = args[1] ? Number.parseInt(args[1], 10) : 5;
      return { outputCol, fn: fnName, sourceCol, windowSize };
    }

    default:
      throw new InvalidArgumentError(`Unknown window function: "${fnName}"`);
  }
}

interface PartitionState {
  rowCount: number;
  lagBuffers: Map<string, unknown[]>;
  movingBuffers: Map<string, number[]>;
  runningSums: Map<string, number>;
  runningCounts: Map<string, number>;
  runningMins: Map<string, number>;
  runningMaxs: Map<string, number>;
}

function createPartitionState(): PartitionState {
  return {
    rowCount: 0,
    lagBuffers: new Map(),
    movingBuffers: new Map(),
    runningSums: new Map(),
    runningCounts: new Map(),
    runningMins: new Map(),
    runningMaxs: new Map(),
  };
}

/**
 * Streaming Window / Rolling Function Transform.
 * Operates over sliding ring buffers and running accumulators with bounded O(W) / O(1) memory.
 */
export function windowRows(options: WindowOptions): TransformFunction {
  const specs = parseWindowSpecs(options.specs);
  const byCols = options.by
    ? Array.isArray(options.by)
      ? options.by
      : options.by.split(",").map((s) => s.trim()).filter(Boolean)
    : undefined;
  const effectiveBatchSize = Math.max(1, options.batchSize || 1000);

  // Determine maximum lookahead needed for lead()
  let maxLeadOffset = 0;
  for (const s of specs) {
    if (s.fn === "lead" && s.offset && s.offset > maxLeadOffset) {
      maxLeadOffset = s.offset;
    }
  }

  return (stream: DataStream): DataStream => {
    return (async function* () {
      const partitions = new Map<string, PartitionState>();
      let currentBatch: Row[] = [];
      let globalOffset = 0;

      // Lookahead buffer for lead() calculations: array of { rawRow, partitionKey }
      const lookaheadQueue: Array<{ row: Row; partitionKey: string }> = [];

      function getPartitionKey(row: Row): string {
        if (!byCols || byCols.length === 0) return "__global__";
        return encodeCompositeKey(row, byCols).encoded;
      }

      function processRow(row: Row, partitionKey: string, lookaheadItems: Array<{ row: Row; partitionKey: string }>): Row {
        let state = partitions.get(partitionKey);
        if (!state) {
          state = createPartitionState();
          partitions.set(partitionKey, state);
        }

        state.rowCount++;
        const enrichedRow: Row = { ...row };

        for (const spec of specs) {
          switch (spec.fn) {
            case "row_number":
            case "rank":
            case "dense_rank":
              enrichedRow[spec.outputCol] = state.rowCount;
              break;

            case "running_count":
              enrichedRow[spec.outputCol] = state.rowCount;
              break;

            case "lag": {
              const src = spec.sourceCol!;
              const offset = spec.offset || 1;
              let lagBuf = state.lagBuffers.get(spec.outputCol);
              if (!lagBuf) {
                lagBuf = [];
                state.lagBuffers.set(spec.outputCol, lagBuf);
              }

              if (lagBuf.length >= offset) {
                enrichedRow[spec.outputCol] = lagBuf[lagBuf.length - offset];
              } else {
                enrichedRow[spec.outputCol] = spec.defaultValue !== undefined ? spec.defaultValue : null;
              }

              lagBuf.push(row[src]);
              if (lagBuf.length > offset * 2) {
                lagBuf.splice(0, lagBuf.length - offset);
              }
              break;
            }

            case "lead": {
              const src = spec.sourceCol!;
              const offset = spec.offset || 1;
              // Find matching partition lookahead item
              let matchIdx = -1;
              let matchCount = 0;
              for (let i = 0; i < lookaheadItems.length; i++) {
                if (lookaheadItems[i]!.partitionKey === partitionKey) {
                  matchCount++;
                  if (matchCount === offset) {
                    matchIdx = i;
                    break;
                  }
                }
              }

              if (matchIdx >= 0) {
                enrichedRow[spec.outputCol] = lookaheadItems[matchIdx]!.row[src];
              } else {
                enrichedRow[spec.outputCol] = spec.defaultValue !== undefined ? spec.defaultValue : null;
              }
              break;
            }

            case "running_sum": {
              const src = spec.sourceCol!;
              const val = Number(row[src]) || 0;
              const current = (state.runningSums.get(spec.outputCol) || 0) + val;
              state.runningSums.set(spec.outputCol, current);
              enrichedRow[spec.outputCol] = current;
              break;
            }

            case "running_avg": {
              const src = spec.sourceCol!;
              const val = Number(row[src]) || 0;
              const currentSum = (state.runningSums.get(spec.outputCol) || 0) + val;
              const count = (state.runningCounts.get(spec.outputCol) || 0) + 1;
              state.runningSums.set(spec.outputCol, currentSum);
              state.runningCounts.set(spec.outputCol, count);
              enrichedRow[spec.outputCol] = count > 0 ? currentSum / count : 0;
              break;
            }

            case "running_min": {
              const src = spec.sourceCol!;
              const val = Number(row[src]);
              if (!Number.isNaN(val)) {
                const currentMin = state.runningMins.get(spec.outputCol);
                const newMin = currentMin !== undefined ? Math.min(currentMin, val) : val;
                state.runningMins.set(spec.outputCol, newMin);
                enrichedRow[spec.outputCol] = newMin;
              } else {
                enrichedRow[spec.outputCol] = null;
              }
              break;
            }

            case "running_max": {
              const src = spec.sourceCol!;
              const val = Number(row[src]);
              if (!Number.isNaN(val)) {
                const currentMax = state.runningMaxs.get(spec.outputCol);
                const newMax = currentMax !== undefined ? Math.max(currentMax, val) : val;
                state.runningMaxs.set(spec.outputCol, newMax);
                enrichedRow[spec.outputCol] = newMax;
              } else {
                enrichedRow[spec.outputCol] = null;
              }
              break;
            }

            case "moving_sum":
            case "moving_avg":
            case "moving_min":
            case "moving_max": {
              const src = spec.sourceCol!;
              const wSize = Math.max(1, spec.windowSize || 5);
              let buf = state.movingBuffers.get(spec.outputCol);
              if (!buf) {
                buf = [];
                state.movingBuffers.set(spec.outputCol, buf);
              }

              const val = Number(row[src]) || 0;
              buf.push(val);
              if (buf.length > wSize) {
                buf.shift();
              }

              if (spec.fn === "moving_sum") {
                enrichedRow[spec.outputCol] = buf.reduce((a, b) => a + b, 0);
              } else if (spec.fn === "moving_avg") {
                enrichedRow[spec.outputCol] = buf.reduce((a, b) => a + b, 0) / buf.length;
              } else if (spec.fn === "moving_min") {
                enrichedRow[spec.outputCol] = Math.min(...buf);
              } else if (spec.fn === "moving_max") {
                enrichedRow[spec.outputCol] = Math.max(...buf);
              }
              break;
            }
          }
        }

        return enrichedRow;
      }

      for await (const batch of stream) {
        for (const row of batch.rows) {
          const partitionKey = getPartitionKey(row);

          if (maxLeadOffset > 0) {
            lookaheadQueue.push({ row, partitionKey });

            if (lookaheadQueue.length > maxLeadOffset) {
              const target = lookaheadQueue.shift()!;
              const enriched = processRow(target.row, target.partitionKey, lookaheadQueue);
              currentBatch.push(enriched);
            }
          } else {
            const enriched = processRow(row, partitionKey, []);
            currentBatch.push(enriched);
          }

          if (currentBatch.length >= effectiveBatchSize) {
            yield {
              rows: currentBatch,
              offset: globalOffset,
            };
            globalOffset += currentBatch.length;
            currentBatch = [];
          }
        }
      }

      // Drain remaining items from lookaheadQueue
      while (lookaheadQueue.length > 0) {
        const target = lookaheadQueue.shift()!;
        const enriched = processRow(target.row, target.partitionKey, lookaheadQueue);
        currentBatch.push(enriched);

        if (currentBatch.length >= effectiveBatchSize) {
          yield {
            rows: currentBatch,
            offset: globalOffset,
          };
          globalOffset += currentBatch.length;
          currentBatch = [];
        }
      }

      if (currentBatch.length > 0) {
        yield {
          rows: currentBatch,
          offset: globalOffset,
        };
        globalOffset += currentBatch.length;
      }
    })();
  };
}
