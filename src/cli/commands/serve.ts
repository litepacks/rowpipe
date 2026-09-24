import { startApiServer, type ServerOptions } from "../../dataops/server.js";
import { createReader, inferFormatFromPath as inferReaderFormat } from "../../readers/index.js";
import { openReadableStream } from "../../utils/compression.js";
import type { UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ServeCommandOptions extends UnifiedPipelineCliOptions {
  port?: number | string;
  host?: string;
}

export async function serveCommand(
  inputPath = "-",
  options: ServeCommandOptions = {}
): Promise<void> {
  const port = Number(options.port) || 3000;
  const host = options.host || "0.0.0.0";
  const effectiveBatchSize = Number(options.batchSize) || 1000;

  let fromFormat = options.from?.toLowerCase();
  if (!fromFormat && inputPath !== "-") {
    fromFormat = inferReaderFormat(inputPath) ?? undefined;
  }
  if (!fromFormat) fromFormat = "csv";

  const readerFactory = () => {
    const readerInput = inputPath === "-" ? openReadableStream("-", options) : inputPath;
    return createReader(readerInput, {
      format: fromFormat,
      sheet: options.sheet,
      delimiter: options.delimiter,
      filePath: inputPath,
      ...options,
      batchSize: effectiveBatchSize,
    });
  };

  const server = startApiServer({
    port,
    host,
    readerFactory,
    filePath: inputPath,
  });

  const displayHost = host === "0.0.0.0" ? "localhost" : host;
  process.stdout.write(`\n🚀 Rowpipe API Server running at http://${displayHost}:${port}/\n`);
  process.stdout.write(`   ├── GET /          (API Metadata & Documentation)\n`);
  process.stdout.write(`   ├── GET /rows      (Stream rows: ?limit=100&offset=0&select=a,b&filter=...)\n`);
  process.stdout.write(`   ├── GET /schema    (Inferred schema and column types)\n`);
  process.stdout.write(`   └── GET /stats     (Streaming Welford numeric statistics)\n\n`);

  process.on("SIGINT", () => {
    server.close(() => {
      process.exit(0);
    });
  });
}
