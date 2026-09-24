import { createServer, IncomingMessage, ServerResponse, Server } from "node:http";
import { URL } from "node:url";
import type { TabularReader, Row } from "../core/types.js";
import { createPipeline } from "../core/pipeline.js";
import { filterRows } from "../transforms/filter.js";
import { selectColumns } from "../transforms/select.js";
import { limitRows } from "../transforms/limit.js";
import { offsetRows } from "../transforms/offset.js";
import { DatasetStatsAggregator } from "../analytics/stats.js";
import { SchemaInferenceAggregator } from "../analytics/schema-inference.js";

export interface ServerOptions {
  port?: number;
  host?: string;
  readerFactory: () => TabularReader;
  filePath?: string;
}

export function startApiServer(options: ServerOptions): Server {
  const port = typeof options.port === "number" ? options.port : 3000;
  const host = options.host || "0.0.0.0";

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    try {
      const parsedUrl = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
      const pathname = parsedUrl.pathname;
      const searchParams = parsedUrl.searchParams;

      // Enable CORS
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type");

      if (req.method === "OPTIONS") {
        res.writeHead(204);
        res.end();
        return;
      }

      if (pathname === "/") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(
          JSON.stringify(
            {
              service: "Rowpipe Data API",
              file: options.filePath || "dataset",
              endpoints: {
                "/rows": "Stream rows. Params: limit, offset, filter, select, format (json|csv)",
                "/schema": "Get inferred dataset schema and types",
                "/stats": "Get streaming numeric statistics and metrics",
              },
            },
            null,
            2
          )
        );
        return;
      }

      if (pathname === "/schema") {
        const reader = options.readerFactory();
        const schemaAgg = new SchemaInferenceAggregator();
        for await (const batch of reader.read()) {
          for (const row of batch.rows) {
            schemaAgg.add(row);
          }
        }
        const schema = schemaAgg.result();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(schema, null, 2));
        if (reader.close) await reader.close();
        return;
      }

      if (pathname === "/stats") {
        const reader = options.readerFactory();
        const statsAgg = new DatasetStatsAggregator();
        for await (const batch of reader.read()) {
          for (const row of batch.rows) {
            statsAgg.add(row);
          }
        }
        const stats = statsAgg.result();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify(stats, null, 2));
        if (reader.close) await reader.close();
        return;
      }

      if (pathname === "/rows") {
        const reader = options.readerFactory();
        let pipeline = createPipeline(reader);

        const filterExpr = searchParams.get("filter");
        if (filterExpr) {
          pipeline = pipeline.pipe(filterRows(filterExpr));
        }

        const selectCols = searchParams.get("select");
        if (selectCols) {
          pipeline = pipeline.pipe(selectColumns(selectCols.split(",").map((s) => s.trim())));
        }

        const offsetVal = searchParams.get("offset");
        if (offsetVal) {
          pipeline = pipeline.pipe(offsetRows(parseInt(offsetVal, 10)));
        }

        const limitVal = searchParams.get("limit");
        if (limitVal) {
          pipeline = pipeline.pipe(limitRows(parseInt(limitVal, 10)));
        }

        const format = searchParams.get("format")?.toLowerCase() || "json";

        if (format === "csv") {
          res.writeHead(200, { "Content-Type": "text/csv; charset=utf-8" });
          let headerSent = false;

          for await (const batch of pipeline.batches()) {
            for (const row of batch.rows) {
              if (!headerSent) {
                const keys = Object.keys(row);
                res.write(keys.join(",") + "\n");
                headerSent = true;
              }
              const values = Object.values(row).map((v) =>
                v === null || v === undefined ? "" : typeof v === "string" && v.includes(",") ? `"${v}"` : String(v)
              );
              res.write(values.join(",") + "\n");
            }
          }
          res.end();
        } else {
          res.writeHead(200, { "Content-Type": "application/json" });
          res.write("[\n");
          let first = true;

          for await (const batch of pipeline.batches()) {
            for (const row of batch.rows) {
              if (!first) {
                res.write(",\n");
              }
              res.write(JSON.stringify(row));
              first = false;
            }
          }
          res.write("\n]");
          res.end();
        }

        if (reader.close) await reader.close();
        return;
      }

      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Endpoint not found" }));
    } catch (err: unknown) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: (err as Error).message }));
    }
  });

  server.listen(port, host);
  return server;
}
