import {
  MemoryClassification,
  PipelineOperation,
  PlannedPipeline,
} from "./types.js";

/**
 * Optimizes an operations sequence with rule-based transformations:
 * 1. Sort + Limit -> TopK rewrite (converting O(N log N) external sort to O(K) heap)
 * 2. Offset + Limit bound calculation for early upstream cancellation
 * 3. Memory classification assessment
 */
export function optimizePipeline(operations: PipelineOperation[]): PlannedPipeline {
  const optimized: PipelineOperation[] = [];
  const optimizationsApplied: string[] = [];

  let i = 0;
  while (i < operations.length) {
    const current = operations[i]!;
    const nextOp = operations[i + 1];

    // Optimization Rule 1: Sort + Limit -> TopK
    if (current.type === "sort" && nextOp && nextOp.type === "limit") {
      const primarySpec = current.specs[0];
      const isDesc = primarySpec ? primarySpec.direction === "desc" : false;

      optimized.push({
        type: "top",
        specs: current.specs,
        count: nextOp.count,
        order: isDesc ? "desc" : "asc",
        smallest: !isDesc,
        nulls: current.nulls,
        ignoreCase: current.ignoreCase,
        natural: current.natural,
      });

      const sortSummary = current.specs.map((s) => `${s.column} ${s.direction || "asc"}`).join(", ");
      optimizationsApplied.push(`sort(${sortSummary}) + limit(${nextOp.count}) -> top-k (${nextOp.count})`);
      i += 2;
      continue;
    }

    optimized.push(current);
    i++;
  }

  // Calculate upstream cancellation limit
  let effectiveReadLimit: number | undefined;

  let offsetAmount = 0;
  let hasOffset = false;

  for (const op of optimized) {
    if (op.type === "offset") {
      offsetAmount += op.count;
      hasOffset = true;
    } else if (op.type === "limit") {
      const totalNeeded = (hasOffset ? offsetAmount : 0) + op.count;
      effectiveReadLimit = effectiveReadLimit !== undefined ? Math.min(effectiveReadLimit, totalNeeded) : totalNeeded;
    } else if (op.type === "sort" || op.type === "tail" || op.type === "group") {
      // Global operations need all rows, cannot stop early before them
      effectiveReadLimit = undefined;
      break;
    }
  }

  if (hasOffset && effectiveReadLimit !== undefined) {
    optimizationsApplied.push(`offset(${offsetAmount}) + limit -> upstream stream capped at ${effectiveReadLimit} rows`);
  }

  // Assess memory classification
  let memoryClassification: MemoryClassification = "streaming";

  for (const op of optimized) {
    if (op.type === "sort" || op.type === "unique" || op.type === "group") {
      memoryClassification = "spillable-global";
      break;
    }
    if (op.type === "top" || op.type === "tail") {
      if (memoryClassification === "streaming") {
        memoryClassification = "bounded-state";
      }
    }
  }

  return {
    operations: optimized,
    optimizationsApplied,
    memoryClassification,
    effectiveReadLimit,
  };
}
