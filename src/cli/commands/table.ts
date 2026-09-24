import { unifiedPipelineCommand, type UnifiedPipelineCliOptions } from "./pipeline.js";
import type { TableWriterOptions } from "../../writers/table.js";

export interface TableCommandOptions extends UnifiedPipelineCliOptions, TableWriterOptions {
  style?: "unicode" | "ascii" | "compact";
  maxColWidth?: number;
  maxBuffer?: number;
}

export async function tableCommand(
  inputPath = "-",
  options: TableCommandOptions = {}
): Promise<void> {
  await unifiedPipelineCommand(inputPath, {
    ...options,
    to: "table",
    maxBufferRows: options.maxBuffer ?? options.maxBufferRows,
  } as any);
}
