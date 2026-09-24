import { createMcpServer, type McpApp } from "mcponce";
import { VERSION } from "../version.js";
import { allRowpipeTools, inspectToolDefinition, schemaToolDefinition } from "./tools.js";

export interface RowpipeMcpServerOptions {
  name?: string;
  version?: string;
  registerInCentral?: boolean;
  background?: boolean;
}

/**
 * Creates and initializes a rowpipe MCP Server application with all tools
 * and resource templates registered.
 */
export function createRowpipeMcpServer(options: RowpipeMcpServerOptions = {}): McpApp {
  const app = createMcpServer({
    name: options.name || "rowpipe",
    version: options.version || VERSION,
    registerInCentral: options.registerInCentral ?? true,
    background: options.background ?? false,
    toolTimeoutMs: 120_000, // 2 minutes for processing large tabular datasets
  });

  // Register all tabular data tools
  for (const toolDef of allRowpipeTools) {
    app.tool(toolDef as any);
  }

  // Register Resource Templates
  app.resourceTemplate({
    uriTemplate: "rowpipe://metadata/{filePath}",
    name: "Dataset Metadata",
    description: "Metadata, format, size, and sheet information for a tabular dataset",
    mimeType: "application/json",
    handler: async (uri, params) => {
      const rawPath = params.filePath ?? "";
      const filePath = decodeURIComponent(rawPath);
      const res = await inspectToolDefinition.handler({ filePath });
      return {
        contents: [
          {
            uri: uri.toString(),
            mimeType: "application/json",
            text: JSON.stringify(res, null, 2),
          },
        ],
      };
    },
  });

  app.resourceTemplate({
    uriTemplate: "rowpipe://schema/{filePath}",
    name: "Dataset Schema",
    description: "Inferred column types, null rates, and semantic types for a tabular dataset",
    mimeType: "application/json",
    handler: async (uri, params) => {
      const rawPath = params.filePath ?? "";
      const filePath = decodeURIComponent(rawPath);
      const res = await schemaToolDefinition.handler({ filePath });
      return {
        contents: [
          {
            uri: uri.toString(),
            mimeType: "application/json",
            text: JSON.stringify(res, null, 2),
          },
        ],
      };
    },
  });

  return app;
}
