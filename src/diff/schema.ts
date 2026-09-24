import type { ColumnType, TabularReader } from "../core/types.js";
import { classifyPrimitiveType } from "../analytics/schema-inference.js";
import type { SchemaColumnDiff } from "./types.js";

interface ColumnInfo {
  name: string;
  type: ColumnType;
}

async function extractReaderColumns(reader: TabularReader): Promise<Map<string, ColumnType>> {
  const map = new Map<string, ColumnType>();

  // 1. Try reader.inspect()
  if (typeof reader.inspect === "function") {
    try {
      const meta = await reader.inspect({ maxRows: 1000 });
      if (meta.columns && meta.columns.length > 0) {
        for (const col of meta.columns) {
          map.set(col.name, col.type);
        }
        return map;
      }
    } catch {
      // Fallback to reading first batch
    }
  }

  // 2. Read first batch
  try {
    for await (const batch of reader.read({ batchSize: 100 })) {
      if (batch.rows.length > 0) {
        const firstRow = batch.rows[0]!;
        for (const [k, v] of Object.entries(firstRow)) {
          map.set(k, classifyPrimitiveType(v));
        }
        break;
      }
    }
  } catch {
    // Ignore error
  }

  return map;
}

/**
 * Computes the schema differences between two tabular readers.
 */
export async function computeSchemaDiff(
  leftReader: TabularReader,
  rightReader: TabularReader
): Promise<SchemaColumnDiff> {
  const leftCols = await extractReaderColumns(leftReader);
  const rightCols = await extractReaderColumns(rightReader);

  const added: Array<{ name: string; type: ColumnType }> = [];
  const removed: Array<{ name: string; type: ColumnType }> = [];
  const changed: Array<{ name: string; leftType: ColumnType; rightType: ColumnType }> = [];

  // Detect removed & changed
  for (const [name, leftType] of leftCols.entries()) {
    if (!rightCols.has(name)) {
      removed.push({ name, type: leftType });
    } else {
      const rightType = rightCols.get(name)!;
      if (leftType !== rightType && leftType !== "mixed" && rightType !== "mixed") {
        changed.push({ name, leftType, rightType });
      }
    }
  }

  // Detect added
  for (const [name, rightType] of rightCols.entries()) {
    if (!leftCols.has(name)) {
      added.push({ name, type: rightType });
    }
  }

  return { added, removed, changed };
}
