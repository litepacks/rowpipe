import { formatExecutionPlan, optimizePipeline } from "../../planner/index.js";
import { isDatabaseUrl, parseDatabaseUrl, sanitizeConnectionString } from "../../db/url.js";
import { analyzeDatabasePushdown } from "../../db/pushdown.js";
import { buildOperationsFromOptions, UnifiedPipelineCliOptions } from "./pipeline.js";

export interface ExplainCliOptions extends UnifiedPipelineCliOptions {
  table?: string;
  query?: string;
  where?: string;
  toDb?: string;
  toTable?: string;
}

export async function explainCommand(
  inputOrArgs: string | string[] = "-",
  options: ExplainCliOptions = {}
): Promise<void> {
  let inputPath = "-";
  if (Array.isArray(inputOrArgs)) {
    if (inputOrArgs[0] === "db") {
      inputPath = inputOrArgs[1] || "-";
    } else {
      inputPath = inputOrArgs[0] || "-";
    }
  } else {
    inputPath = inputOrArgs;
  }

  const operations = buildOperationsFromOptions(options);

  // Check if input is a database URL or table mode
  if (isDatabaseUrl(inputPath) || options.table || options.query) {
    let sanitizedDb = "Database";
    let dialect: any = "sqlite";
    if (isDatabaseUrl(inputPath)) {
      const cfg = parseDatabaseUrl(inputPath);
      sanitizedDb = cfg.sanitizedUrl;
      dialect = cfg.dialect;
    }

    if (options.table && !options.query) {
      const analysis = analyzeDatabasePushdown(options.table, dialect, operations, options.where);
      const optimizedPlan = optimizePipeline(analysis.remainingOperations);

      let out = "\n======================================================\n";
      out += "              Rowpipe Execution Plan                  \n";
      out += "======================================================\n\n";

      out += `DatabaseSource (${sanitizedDb})\n`;
      out += `  Table: ${options.table}\n`;
      out += `  Dialect: ${dialect}\n\n`;

      out += "Remote Database Pushdown:\n";
      out += `  SQL: ${analysis.generatedQuery}\n`;
      if (analysis.pushedSummary.length > 0) {
        for (const s of analysis.pushedSummary) {
          out += `  • ${s}\n`;
        }
      } else {
        out += "  • SELECT *\n";
      }

      out += "\nLocal Stream Operations:\n";
      if (optimizedPlan.operations.length === 0) {
        out += "  (none - full pushdown achieved!)\n";
      } else {
        for (const op of optimizedPlan.operations) {
          out += `  • ${op.type}\n`;
        }
      }

      out += "\nOutput Target:\n";
      if (options.toDb) {
        out += `  DatabaseSink (${sanitizeConnectionString(options.toDb)}) -> Table: ${options.toTable || options.table}\n`;
      } else if (options.output) {
        out += `  Writer (${options.output})\n`;
      } else {
        out += "  Stdout Writer\n";
      }

      out += "\n------------------------------------------------------\n";
      if (optimizedPlan.optimizationsApplied.length > 0) {
        out += "Local Optimizations Applied:\n";
        for (const opt of optimizedPlan.optimizationsApplied) {
          out += `  • ${opt}\n`;
        }
        out += "------------------------------------------------------\n";
      }

      out += "Memory Classification:\n";
      if (optimizedPlan.memoryClassification === "streaming") {
        out += "  Fully Streaming (Cursor-based bounded memory)\n";
      } else {
        out += `  ${optimizedPlan.memoryClassification}\n`;
      }
      out += "======================================================\n\n";

      process.stdout.write(out);
      return;
    }
  }

  const plan = optimizePipeline(operations);
  const inputDesc = inputPath === "-" ? "Stdin Stream" : `Reader (${inputPath})`;
  let outputDesc = "Stdout Writer";
  if (options.toDb) {
    outputDesc = `DatabaseSink (${sanitizeConnectionString(options.toDb)}) -> Table: ${options.toTable || "target"}`;
  } else if (options.output) {
    outputDesc = `Writer (${options.output})`;
  }

  const formatted = formatExecutionPlan(plan, inputDesc, outputDesc);
  process.stdout.write(formatted);
}
