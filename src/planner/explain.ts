import { PlannedPipeline } from "./types.js";

/**
 * Formats a clean ASCII representation of the planned execution pipeline.
 */
export function formatExecutionPlan(
  plan: PlannedPipeline,
  inputDescription = "Input Stream",
  outputDescription = "Output Stream"
): string {
  const steps: string[] = [inputDescription];

  for (const op of plan.operations) {
    switch (op.type) {
      case "filter":
        steps.push(`Filter (${op.expression})`);
        break;

      case "select":
        steps.push(`Select (${op.columns.join(", ")})`);
        break;

      case "rename":
        const renames = Object.entries(op.specs).map(([o, n]) => `${o}->${n}`).join(", ");
        steps.push(`Rename (${renames})`);
        break;

      case "cast":
        const casts = Object.entries(op.specs).map(([c, t]) => `${c}:${t}`).join(", ");
        steps.push(`Cast (${casts})`);
        break;

      case "map":
        const maps = Object.entries(op.specs).map(([k, v]) => `${k}=${v}`).join(", ");
        steps.push(`Map (${maps})`);
        break;

      case "sort":
        const sorts = op.specs.map((s) => `${s.column} ${(s.direction || "asc").toUpperCase()}`).join(", ");
        steps.push(`ExternalSort (${sorts}) [Spillable]`);
        break;

      case "top":
        const topCols = op.specs.map((s) => `${s.column} ${(s.direction || (op.order === "asc" ? "ASC" : "DESC")).toUpperCase()}`).join(", ");
        steps.push(`TopK (${topCols}, ${op.count}) [Bounded Heap]`);
        break;

      case "limit":
        steps.push(`Limit (${op.count}) [Early Stream Termination]`);
        break;

      case "offset":
        steps.push(`Offset (skip ${op.count})`);
        break;

      case "tail":
        steps.push(`Tail (last ${op.count}) [Bounded RingBuffer]`);
        break;

      case "unique":
        const byStr = op.by ? op.by.join(", ") : "whole row";
        steps.push(`Unique (by: ${byStr}, keep: ${op.keep || "first"}) [Spillable]`);
        break;

      case "group":
        const gByStr = op.by ? op.by.join(", ") : "global";
        const aggsStr = op.aggregations.map((a) => `${a.targetField}=${a.func}(${a.sourceExpr || ""})`).join(", ");
        steps.push(`Group (by: ${gByStr}, aggs: ${aggsStr}) [Spillable]`);
        break;
    }
  }

  steps.push(outputDescription);

  let output = "\n======================================================\n";
  output += "              Rowpipe Execution Plan                  \n";
  output += "======================================================\n\n";

  for (let i = 0; i < steps.length; i++) {
    output += `  ${steps[i]}\n`;
    if (i < steps.length - 1) {
      output += "    ↓\n";
    }
  }

  output += "\n------------------------------------------------------\n";

  if (plan.optimizationsApplied.length > 0) {
    output += "Optimizations Applied:\n";
    for (const opt of plan.optimizationsApplied) {
      output += `  • ${opt}\n`;
    }
  } else {
    output += "Optimizations Applied:\n  (none)\n";
  }

  output += "------------------------------------------------------\n";
  output += "Memory Classification:\n";
  if (plan.memoryClassification === "streaming") {
    output += "  Fully Streaming (O(1) constant memory)\n";
  } else if (plan.memoryClassification === "bounded-state") {
    output += "  Bounded State (O(K) fixed memory bound)\n";
  } else {
    output += "  Spillable Global State (O(1) RAM threshold + Disk Spill)\n";
  }

  if (plan.effectiveReadLimit !== undefined) {
    output += `Upstream Read Cap: ${plan.effectiveReadLimit} rows\n`;
  }
  output += "======================================================\n\n";

  return output;
}
