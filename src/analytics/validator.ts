import type { Aggregator, Row } from "../core/types.js";
import { classifyPrimitiveType } from "./schema-inference.js";
import { detectSemanticType } from "./semantic-types.js";

export interface ColumnSchemaRule {
  type?: string;
  nullable?: boolean;
  format?: string;
}

export type ValidationSchemaDefinition = Record<string, ColumnSchemaRule>;

export interface ColumnViolationSummary {
  column: string;
  rule: string;
  violationsCount: number;
  sampleInvalidValues: unknown[];
}

export interface ValidationReport {
  totalRows: number;
  validRows: number;
  invalidRows: number;
  isValid: boolean;
  violations: ColumnViolationSummary[];
}

export class SchemaValidatorAggregator implements Aggregator<ValidationReport> {
  private schema: ValidationSchemaDefinition;
  private totalRows = 0;
  private validRows = 0;
  private invalidRows = 0;
  private violationsMap = new Map<string, ColumnViolationSummary>();

  constructor(schema: ValidationSchemaDefinition) {
    this.schema = schema;
  }

  private addViolation(column: string, rule: string, value: unknown): void {
    const key = `${column}::${rule}`;
    if (!this.violationsMap.has(key)) {
      this.violationsMap.set(key, {
        column,
        rule,
        violationsCount: 0,
        sampleInvalidValues: [],
      });
    }

    const v = this.violationsMap.get(key)!;
    v.violationsCount++;
    if (v.sampleInvalidValues.length < 5) {
      v.sampleInvalidValues.push(value);
    }
  }

  add(row: Row): void {
    this.totalRows++;
    let rowHasError = false;

    for (const [colName, rule] of Object.entries(this.schema)) {
      const val = row[colName];
      const isNull = val === null || val === undefined || val === "";

      // Nullability check
      if (isNull) {
        if (rule.nullable === false) {
          this.addViolation(colName, "null not allowed", val);
          rowHasError = true;
        }
        continue;
      }

      // Type check
      if (rule.type) {
        const actualType = classifyPrimitiveType(val);
        const expectedType = rule.type.toLowerCase();

        let matchesType = false;
        if (expectedType === "string") {
          matchesType = true;
        } else if (expectedType === "integer" || expectedType === "int") {
          matchesType = actualType === "integer";
        } else if (expectedType === "number" || expectedType === "float") {
          matchesType = actualType === "integer" || actualType === "number";
        } else if (expectedType === "boolean" || expectedType === "bool") {
          matchesType = actualType === "boolean";
        } else if (expectedType === "date") {
          matchesType = actualType === "date" || actualType === "datetime";
        } else if (expectedType === "datetime") {
          matchesType = actualType === "datetime";
        }

        if (!matchesType) {
          this.addViolation(
            colName,
            `expected ${rule.type}`,
            val
          );
          rowHasError = true;
        }
      }

      // Format check (semantic type)
      if (rule.format) {
        const detected = detectSemanticType(val);
        if (detected !== rule.format.toLowerCase()) {
          this.addViolation(
            colName,
            `expected format ${rule.format}`,
            val
          );
          rowHasError = true;
        }
      }
    }

    if (rowHasError) {
      this.invalidRows++;
    } else {
      this.validRows++;
    }
  }

  result(): ValidationReport {
    return {
      totalRows: this.totalRows,
      validRows: this.validRows,
      invalidRows: this.invalidRows,
      isValid: this.invalidRows === 0,
      violations: Array.from(this.violationsMap.values()),
    };
  }
}
