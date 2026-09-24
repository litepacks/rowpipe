import { InvalidArgumentError } from "../core/errors.js";
import type { DataStream, Row, TransformFunction } from "../core/types.js";

export type CastOnError = "fail" | "null" | "keep" | "skip-row";

export interface CastOptions {
  onError?: CastOnError;
}

/**
 * Casts a single value to the target type.
 */
export function castValue(
  value: unknown,
  targetType: string,
  onError: CastOnError = "null"
): { value: unknown; success: boolean } {
  if (value === null || value === undefined || value === "") {
    return { value: null, success: true };
  }

  const str = String(value).trim();
  const lowerType = targetType.toLowerCase();

  try {
    switch (lowerType) {
      case "string":
        return { value: String(value), success: true };

      case "integer":
      case "int": {
        if (!/^-?\d+$/.test(str)) {
          throw new Error(`Cannot cast "${str}" to integer`);
        }
        const parsedInt = Number.parseInt(str, 10);
        if (Number.isNaN(parsedInt)) {
          throw new Error(`Cannot cast "${str}" to integer`);
        }
        return { value: parsedInt, success: true };
      }

      case "number":
      case "float":
      case "double": {
        const parsedNum = Number.parseFloat(str);
        if (Number.isNaN(parsedNum)) {
          throw new Error(`Cannot cast "${str}" to number`);
        }
        return { value: parsedNum, success: true };
      }

      case "boolean":
      case "bool": {
        const lower = str.toLowerCase();
        if (["true", "1", "yes", "t", "y"].includes(lower)) {
          return { value: true, success: true };
        }
        if (["false", "0", "no", "f", "n"].includes(lower)) {
          return { value: false, success: true };
        }
        throw new Error(`Cannot cast "${str}" to boolean`);
      }

      case "date": {
        const d = new Date(str);
        if (Number.isNaN(d.getTime())) {
          throw new Error(`Cannot cast "${str}" to date`);
        }
        return { value: d.toISOString().split("T")[0], success: true };
      }

      case "datetime": {
        const d = new Date(str);
        if (Number.isNaN(d.getTime())) {
          throw new Error(`Cannot cast "${str}" to datetime`);
        }
        return { value: d.toISOString(), success: true };
      }

      case "json": {
        const parsed = JSON.parse(str);
        return { value: parsed, success: true };
      }

      default:
        throw new Error(`Unknown target type "${targetType}"`);
    }
  } catch (err) {
    if (onError === "fail") {
      throw new InvalidArgumentError(
        `Failed to cast value "${str}" to type "${targetType}": ${(err as Error).message}`
      );
    }
    if (onError === "keep") {
      return { value, success: false };
    }
    // "null" or "skip-row"
    return { value: null, success: false };
  }
}

/**
 * Creates a transform that casts specified columns to target types.
 */
export function castColumns(
  typeSpecs: Record<string, string>,
  options: CastOptions = {}
): TransformFunction {
  const onError = options.onError || "null";

  const compiled = Object.entries(typeSpecs).map(([col, targetType]) => {
    const lower = targetType.toLowerCase();

    // Fast-path specialized caster functions
    if (lower === "number" || lower === "float" || lower === "double") {
      return {
        col,
        cast: (val: unknown) => {
          if (val === null || val === undefined || val === "") return { value: null, success: true };
          const num = typeof val === "number" ? val : Number(val);
          if (!Number.isNaN(num)) return { value: num, success: true };
          if (onError === "fail") throw new InvalidArgumentError(`Cannot cast "${String(val)}" to number`);
          if (onError === "keep") return { value: val, success: false };
          return { value: null, success: false };
        },
      };
    }

    if (lower === "integer" || lower === "int") {
      return {
        col,
        cast: (val: unknown) => {
          if (val === null || val === undefined || val === "") return { value: null, success: true };
          const str = String(val).trim();
          if (!/^-?\d+$/.test(str)) {
            if (onError === "fail") throw new InvalidArgumentError(`Cannot cast "${str}" to integer`);
            if (onError === "keep") return { value: val, success: false };
            return { value: null, success: false };
          }
          const num = Number.parseInt(str, 10);
          return { value: num, success: true };
        },
      };
    }

    if (lower === "string") {
      return {
        col,
        cast: (val: unknown) => ({ value: val !== null && val !== undefined ? String(val) : "", success: true }),
      };
    }

    return {
      col,
      cast: (val: unknown) => castValue(val, targetType, onError),
    };
  });

  const compiledLen = compiled.length;

  return function (stream: DataStream): DataStream {
    return (async function* () {
      for await (const batch of stream) {
        const rows = batch.rows;
        const rowCount = rows.length;
        const transformedRows: Row[] = [];

        for (let i = 0; i < rowCount; i++) {
          const row = rows[i]!;
          let skipRow = false;
          const newRow: Row = { ...row };

          for (let j = 0; j < compiledLen; j++) {
            const entry = compiled[j]!;
            const col = entry.col;
            if (col in newRow) {
              const res = entry.cast(newRow[col]);
              if (!res.success && onError === "skip-row") {
                skipRow = true;
                break;
              }
              newRow[col] = res.value;
            }
          }

          if (!skipRow) {
            transformedRows.push(newRow);
          }
        }

        yield {
          rows: transformedRows,
          offset: batch.offset,
        };
      }
    })();
  };
}

/**
 * Parses command line cast specs like ["age:integer", "revenue:number"].
 */
export function parseCastSpecs(specs: string[]): Record<string, string> {
  const mapping: Record<string, string> = {};
  for (const spec of specs) {
    const parts = spec.split(":");
    if (parts.length === 2 && parts[0] && parts[1]) {
      mapping[parts[0].trim()] = parts[1].trim();
    }
  }
  return mapping;
}
