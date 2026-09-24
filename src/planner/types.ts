import type { AggregationSpec } from "../analytics/reduce.js";
import type { SortKeySpec } from "../transforms/sort/comparator.js";

export type MemoryClassification =
  | "streaming"
  | "bounded-state"
  | "spillable-global";

export type PipelineOperation =
  | { type: "filter"; expression: string }
  | { type: "select"; columns: string[] }
  | { type: "rename"; specs: Record<string, string> }
  | { type: "cast"; specs: Record<string, string>; onError?: string }
  | { type: "map"; specs: Record<string, string> }
  | {
      type: "sort";
      specs: SortKeySpec[];
      nulls?: "first" | "last";
      ignoreCase?: boolean;
      natural?: boolean;
      memoryLimit?: number | string;
    }
  | {
      type: "top";
      specs: SortKeySpec[];
      count: number;
      order?: "asc" | "desc";
      smallest?: boolean;
      nulls?: "first" | "last";
      ignoreCase?: boolean;
      natural?: boolean;
    }
  | { type: "limit"; count: number }
  | { type: "offset"; count: number }
  | { type: "tail"; count: number }
  | {
      type: "unique";
      by?: string[];
      keep?: "first" | "last";
      approx?: boolean;
      memoryLimit?: number | string;
    }
  | {
      type: "group";
      by?: string[];
      aggregations: AggregationSpec[];
      memoryLimit?: number | string;
    }
  | {
      type: "window";
      specs: Record<string, string>;
      by?: string[];
    }
  | {
      type: "explode";
      column: string;
      delimiter?: string;
      trim?: boolean;
      dropEmpty?: boolean;
    }
  | {
      type: "flatten";
      separator?: string;
      maxDepth?: number;
      arrays?: boolean;
    };

export interface PlannedPipeline {
  operations: PipelineOperation[];
  optimizationsApplied: string[];
  memoryClassification: MemoryClassification;
  effectiveReadLimit?: number;
}
