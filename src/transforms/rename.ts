import type { DataStream, Row, TransformFunction } from "../core/types.js";

/**
 * Creates a transform that renames specified columns in streaming batches.
 */
export function renameColumns(
  mapping: Record<string, string>
): TransformFunction {
  return function (stream: DataStream): DataStream {
    return (async function* () {
      for await (const batch of stream) {
        const renamedRows: Row[] = new Array(batch.rows.length);

        for (let i = 0; i < batch.rows.length; i++) {
          const row = batch.rows[i]!;
          const newRow: Row = {};

          for (const [key, val] of Object.entries(row)) {
            const targetKey = mapping[key] || key;
            newRow[targetKey] = val;
          }

          renamedRows[i] = newRow;
        }

        yield {
          rows: renamedRows,
          offset: batch.offset,
        };
      }
    })();
  };
}

/**
 * Parses command line rename specs like ["username=name", "signup_date=created_at"].
 */
export function parseRenameSpecs(specs: string[]): Record<string, string> {
  const mapping: Record<string, string> = {};
  for (const spec of specs) {
    const parts = spec.split("=");
    if (parts.length === 2 && parts[0] && parts[1]) {
      mapping[parts[0].trim()] = parts[1].trim();
    }
  }
  return mapping;
}
