import type { TransformFunction } from "../../core/types.js";
import { externalSort } from "./external-sort.js";
import type { SortOptions } from "./comparator.js";

export * from "./comparator.js";
export * from "./external-sort.js";

/**
 * Sorts tabular stream by one or more columns with typed comparisons,
 * stable ordering, and automatic external merge sort on large streams.
 */
export function sortRows(options: SortOptions): TransformFunction {
  return externalSort(options);
}
