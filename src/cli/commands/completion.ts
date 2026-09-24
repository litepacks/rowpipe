import { generateCompletion } from "../completion.js";

export interface CompletionCommandOptions {
  shell?: string;
}

export async function completionCommand(
  shell = "zsh",
  _options: CompletionCommandOptions = {}
): Promise<void> {
  const normalized = (shell || "zsh").toLowerCase();
  const script = generateCompletion(normalized);
  process.stdout.write(script);
}
