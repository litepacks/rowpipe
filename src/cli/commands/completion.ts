import { generateCompletion, installCompletion } from "../completion.js";

export interface CompletionCommandOptions {
  shell?: string;
  install?: boolean;
}

export async function completionCommand(
  shell = "zsh",
  options: CompletionCommandOptions = {}
): Promise<void> {
  if (options.install) {
    const res = await installCompletion(shell);
    if (res.alreadyInstalled) {
      process.stdout.write(
        `ℹ Autocompletion for ${res.shell} is already installed in ${res.targetFile}.\n`
      );
    } else {
      process.stdout.write(
        `✓ Autocompletion for ${res.shell} successfully installed into ${res.targetFile}.\n` +
        `  To activate in your current terminal session, run:\n` +
        `  source "${res.targetFile}"\n`
      );
    }
    return;
  }

  const normalized = (shell || "zsh").toLowerCase();
  const script = generateCompletion(normalized);
  process.stdout.write(script);
}
