import type { ProgressCallbackInfo } from "../core/types.js";
import { formatNumber } from "./formatting.js";

export interface ProgressReporterOptions {
  quiet?: boolean;
  noProgress?: boolean;
}

export class ProgressReporter {
  private isEnabled: boolean;
  private hasReported = false;

  constructor(options: ProgressReporterOptions = {}) {
    this.isEnabled =
      !options.quiet &&
      !options.noProgress &&
      Boolean(process.stderr.isTTY);
  }

  update(info: ProgressCallbackInfo): void {
    if (!this.isEnabled) return;
    this.hasReported = true;

    const rows = formatNumber(info.rowsProcessed);
    const speed = formatNumber(info.rowsPerSecond);
    const timeSec = (info.elapsedMs / 1000).toFixed(1);

    const line = `\r\x1b[2K  Processed ${rows} rows | ${timeSec}s | ${speed} rows/s`;
    process.stderr.write(line);
  }

  done(): void {
    if (this.isEnabled && this.hasReported) {
      process.stderr.write("\r\x1b[2K");
    }
  }
}
