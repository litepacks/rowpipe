import { fileURLToPath } from "node:url";
import { createRowpipeMcpServer, type RowpipeMcpServerOptions } from "./server.js";

export * from "./tools.js";
export * from "./server.js";

/**
 * Runs the rowpipe MCP server CLI / stdio bridge.
 */
export async function runRowpipeMcpServer(options: RowpipeMcpServerOptions = {}): Promise<void> {
  const app = createRowpipeMcpServer(options);
  await app.run();
}

// Standalone execution entrypoint
const currentFilePath = fileURLToPath(import.meta.url);
if (process.argv[1] && currentFilePath.endsWith(process.argv[1])) {
  runRowpipeMcpServer().catch((err) => {
    process.stderr.write(`rowpipe MCP server failed: ${err.message}\n`);
    process.exit(1);
  });
}
