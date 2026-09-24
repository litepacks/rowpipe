import { formatNumber, safeJsonStringify } from "../utils/formatting.js";
import { formatKeyForDisplay } from "./key.js";
import type { DiffEvent, DiffSummary } from "./types.js";

function formatValueForDisplay(val: unknown): string {
  if (val === undefined) return "<missing>";
  if (val === null) return "null";
  if (typeof val === "bigint") return String(val);
  if (typeof val === "object") return safeJsonStringify(val);
  return String(val);
}

/**
 * Formats a single DiffEvent for --format rows output.
 */
export function formatRowEvent(event: DiffEvent): string | null {
  const keyStr = formatKeyForDisplay(event.key);

  switch (event.type) {
    case "added":
      return `ADDED ${keyStr}\n`;

    case "removed":
      return `REMOVED ${keyStr}\n`;

    case "changed": {
      let output = `CHANGED ${keyStr}\n`;
      for (const ch of event.changes) {
        const oldDisplay = formatValueForDisplay(ch.oldVal);
        const newDisplay = formatValueForDisplay(ch.newVal);
        output += `  ${ch.column}: ${oldDisplay} -> ${newDisplay}\n`;
      }
      return output;
    }

    case "unchanged":
      return `UNCHANGED ${keyStr}\n`;

    default:
      return null;
  }
}

/**
 * Formats a single DiffEvent for --format patch (JSONL) output.
 */
export function formatPatchEvent(event: DiffEvent): string | null {
  switch (event.type) {
    case "added":
      return (
        JSON.stringify({
          op: "insert",
          key: event.key,
          row: event.row,
        }) + "\n"
      );

    case "removed":
      return (
        JSON.stringify({
          op: "delete",
          key: event.key,
          row: event.row,
        }) + "\n"
      );

    case "changed": {
      const changesObj: Record<string, { old: unknown; new: unknown }> = {};
      for (const ch of event.changes) {
        changesObj[ch.column] = {
          old: ch.oldVal,
          new: ch.newVal,
        };
      }
      return (
        JSON.stringify({
          op: "update",
          key: event.key,
          changes: changesObj,
        }) + "\n"
      );
    }

    default:
      return null;
  }
}

/**
 * Formats complete DiffSummary as human-readable ASCII text.
 */
export function formatSummary(summary: DiffSummary): string {
  const lines: string[] = [];

  lines.push("Comparing:");
  lines.push(`  ${summary.leftSource}`);
  lines.push(`  ${summary.rightSource}`);
  lines.push("");
  lines.push(`Key: ${summary.keyColumns.join(", ")}`);
  lines.push("");

  lines.push("Rows");
  const maxNumLen = Math.max(
    formatNumber(summary.rows.added).length,
    formatNumber(summary.rows.removed).length,
    formatNumber(summary.rows.changed).length,
    formatNumber(summary.rows.unchanged).length
  );

  lines.push(`  added      ${formatNumber(summary.rows.added).padStart(maxNumLen)}`);
  lines.push(`  removed    ${formatNumber(summary.rows.removed).padStart(maxNumLen)}`);
  lines.push(`  changed    ${formatNumber(summary.rows.changed).padStart(maxNumLen)}`);
  lines.push(`  unchanged  ${formatNumber(summary.rows.unchanged).padStart(maxNumLen)}`);

  // Changed columns breakdown
  const changedCols = Object.entries(summary.columns).sort(
    (a, b) => b[1].changed - a[1].changed
  );

  if (changedCols.length > 0) {
    lines.push("");
    lines.push("Changed columns");
    const maxColLen = Math.max(...changedCols.map(([c]) => c.length), 10);
    for (const [col, info] of changedCols) {
      lines.push(`  ${col.padEnd(maxColLen)}  ${formatNumber(info.changed)}`);
    }
  }

  // Schema diff
  if (summary.schema) {
    const { added, removed, changed } = summary.schema;
    if (added.length > 0 || removed.length > 0 || changed.length > 0) {
      lines.push("");
      lines.push("Schema");
      for (const col of added) {
        lines.push(`  + ${col.name}: ${col.type}`);
      }
      for (const col of changed) {
        lines.push(`  ~ ${col.name}: ${col.leftType} -> ${col.rightType}`);
      }
      for (const col of removed) {
        lines.push(`  - ${col.name}: ${col.type}`);
      }
    }
  }

  return lines.join("\n") + "\n";
}

/**
 * Formats complete DiffSummary as JSON string.
 */
export function formatJson(summary: DiffSummary): string {
  const jsonObject = {
    left: summary.leftSource,
    right: summary.rightSource,
    key: summary.keyColumns,
    rows: summary.rows,
    columns: summary.columns,
    schema: summary.schema,
    executionTimeMs: summary.executionTimeMs,
  };
  return safeJsonStringify(jsonObject, 2) + "\n";
}
