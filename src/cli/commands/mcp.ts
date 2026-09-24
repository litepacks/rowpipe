import { fileURLToPath } from "node:url";
import { createRowpipeMcpServer } from "../../mcp/server.js";

export interface McpCliCommandOptions {
  background?: boolean;
  dev?: boolean;
}

/**
 * Handles `rowpipe mcp [args...]`
 *
 * Forwards arguments to the mcponce MCP application runner, enabling:
 * - stdio bridge mode (`rowpipe mcp`)
 * - daemon background mode (`rowpipe mcp --background`)
 * - developer mode with stderr logging (`rowpipe mcp --dev`)
 * - tool listings and cli invocations (`rowpipe mcp tools`, `rowpipe mcp call ...`)
 * - client installation (`rowpipe mcp install claude`, etc.)
 * - server status, logs, stop, restart (`rowpipe mcp status`, `rowpipe mcp logs`, etc.)
 */
export async function mcpCommand(args: string[] = []): Promise<void> {
  const mcpEntrypoint = fileURLToPath(new URL("../../mcp/index.js", import.meta.url));

  const app = createRowpipeMcpServer({
    name: "rowpipe",
    background: args.includes("--background") || args.includes("-b"),
  });

  // Adjust process.argv for mcponce CLI parsing
  // mcponce inspects process.argv.slice(2)
  const nodeExec = process.argv[0] ?? process.execPath;
  const mcpIdx = process.argv.findIndex((a) => a === "mcp");
  if (mcpIdx !== -1) {
    process.argv = [
      nodeExec,
      mcpEntrypoint,
      ...process.argv.slice(mcpIdx + 1),
    ];
  } else {
    process.argv = [
      nodeExec,
      mcpEntrypoint,
      ...args,
    ];
  }

  await app.run();
}
