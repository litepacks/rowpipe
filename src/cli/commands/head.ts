import { limitCommand, LimitCommandOptions } from "./limit.js";

export interface HeadCommandOptions extends LimitCommandOptions {
  lines?: string | number;
  n?: string | number;
}

export async function headCommand(
  inputPath = "-",
  options: HeadCommandOptions = {}
): Promise<void> {
  const count = Number(options.n ?? options.lines ?? 10) || 10;
  await limitCommand(inputPath, count, options);
}
