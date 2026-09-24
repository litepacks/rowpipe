import { openSync, writeSync, closeSync } from "node:fs";
import { ParseError, RowpipeError } from "./errors.js";
import type { ErrorHandlingStrategy, Row } from "./types.js";

export interface ErrorReportItem {
  timestamp: string;
  errorType: string;
  message: string;
  file?: string;
  sheet?: string;
  row?: number;
  column?: string | number;
  byteOffset?: number;
  raw?: unknown;
}

export interface RowErrorHandlerOptions {
  strategy?: ErrorHandlingStrategy;
  badRowsLog?: string;
  sourceFile?: string;
}

export class RowErrorHandler {
  private strategy: ErrorHandlingStrategy;
  private badRowsLog?: string;
  private sourceFile?: string;
  private errorCount: number = 0;
  private logFd?: number;

  constructor(options: RowErrorHandlerOptions = {}) {
    this.strategy = options.strategy || "abort";
    this.badRowsLog = options.badRowsLog;
    this.sourceFile = options.sourceFile;

    if (this.badRowsLog) {
      try {
        this.logFd = openSync(this.badRowsLog, "a");
      } catch (err) {
        // Fallback to stderr if file opening fails
        process.stderr.write(`[rowpipe error] Failed to open bad-rows log file "${this.badRowsLog}": ${(err as Error).message}\n`);
      }
    }
  }

  get strategyName(): ErrorHandlingStrategy {
    return this.strategy;
  }

  get totalErrors(): number {
    return this.errorCount;
  }

  handle(
    error: Error | RowpipeError,
    context: {
      row?: number;
      column?: string | number;
      byteOffset?: number;
      raw?: unknown;
      file?: string;
      sheet?: string;
    } = {}
  ): void {
    this.errorCount++;
    const file = context.file || this.sourceFile;

    if (this.strategy === "abort" || this.strategy === "fail") {
      if (error instanceof RowpipeError) {
        throw error;
      }
      throw new ParseError(error.message, {
        file,
        row: context.row,
        column: context.column,
        byteOffset: context.byteOffset,
        sheet: context.sheet,
      });
    }

    // strategy is "skip" or "log"
    if (this.strategy === "log" || this.badRowsLog) {
      const record: ErrorReportItem = {
        timestamp: new Date().toISOString(),
        errorType: error.name || "Error",
        message: error.message,
        file,
        sheet: context.sheet,
        row: context.row,
        column: context.column,
        byteOffset: context.byteOffset,
        raw: context.raw,
      };

      const line = JSON.stringify(record) + "\n";
      if (this.logFd !== undefined) {
        try {
          writeSync(this.logFd, line);
        } catch {
          process.stderr.write(`[rowpipe bad-row] ${line}`);
        }
      } else {
        process.stderr.write(`[rowpipe bad-row] ${line}`);
      }
    }
  }

  close(): void {
    if (this.logFd !== undefined) {
      try {
        closeSync(this.logFd);
      } catch {}
      this.logFd = undefined;
    }
  }
}
