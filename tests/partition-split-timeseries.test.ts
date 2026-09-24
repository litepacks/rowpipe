import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import { rmSync, existsSync, readFileSync, readdirSync } from "node:fs";
import { PartitionManager } from "../src/transforms/partition.js";
import { splitStream } from "../src/transforms/split.js";
import {
  resampleTimeseries,
  parseTimeInterval,
  parseTimestamp,
} from "../src/transforms/timeseries.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { Row } from "../src/core/types.js";

const execAsync = promisify(exec);
const cliPath = join(process.cwd(), "dist/cli/index.js");

describe("Partition, Split & Timeseries Suite", () => {
  const scratchDir = join(process.cwd(), "scratch_test_ps");

  beforeEach(() => {
    if (existsSync(scratchDir)) {
      rmSync(scratchDir, { recursive: true, force: true });
    }
  });

  afterEach(() => {
    if (existsSync(scratchDir)) {
      rmSync(scratchDir, { recursive: true, force: true });
    }
  });

  describe("PartitionManager Transform", () => {
    it("should partition stream into dynamic files based on template", async () => {
      const rows: Row[] = [
        { id: 1, country: "US", year: 2020, val: 10 },
        { id: 2, country: "TR", year: 2020, val: 20 },
        { id: 3, country: "US", year: 2021, val: 30 },
        { id: 4, country: "TR", year: 2020, val: 40 },
      ];

      const pattern = `${scratchDir}/{country}/{year}.csv`;
      const manager = new PartitionManager({
        outPattern: pattern,
        maxOpenWriters: 10,
      });

      const pipeline = createPipeline(rows);
      const result = await manager.partition(pipeline.batches());

      expect(result.rowCount).toBe(4);
      expect(result.fileCount).toBe(3); // US/2020, US/2021, TR/2020

      expect(existsSync(`${scratchDir}/US/2020.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/US/2021.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/TR/2020.csv`)).toBe(true);

      const trContent = readFileSync(`${scratchDir}/TR/2020.csv`, "utf8");
      expect(trContent).toContain("TR");
      expect(trContent).toContain("20");
      expect(trContent).toContain("40");
    });

    it("should handle LRU writer eviction without losing rows", async () => {
      const rows: Row[] = [];
      for (let i = 0; i < 30; i++) {
        rows.push({ id: i, group: `grp_${i % 10}`, val: i * 10 });
      }

      const pattern = `${scratchDir}/lru/{group}.csv`;
      const manager = new PartitionManager({
        outPattern: pattern,
        maxOpenWriters: 3, // Very small pool to trigger frequent evictions
      });

      const pipeline = createPipeline(rows);
      const result = await manager.partition(pipeline.batches());

      expect(result.rowCount).toBe(30);
      expect(result.fileCount).toBe(10);

      // Verify all 10 group files exist
      for (let g = 0; g < 10; g++) {
        expect(existsSync(`${scratchDir}/lru/grp_${g}.csv`)).toBe(true);
      }
    });
  });

  describe("splitStream Transform", () => {
    it("should split stream into sequential chunk files by chunk size", async () => {
      const rows: Row[] = [];
      for (let i = 1; i <= 10; i++) {
        rows.push({ id: i, name: `User ${i}` });
      }

      const pattern = `${scratchDir}/parts/chunk_{n:03d}.csv`;
      const pipeline = createPipeline(rows);

      const result = await splitStream(pipeline.batches(), {
        chunkSize: 3,
        outPattern: pattern,
      });

      expect(result.rowCount).toBe(10);
      expect(result.fileCount).toBe(4); // 3 + 3 + 3 + 1

      expect(existsSync(`${scratchDir}/parts/chunk_001.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/parts/chunk_002.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/parts/chunk_003.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/parts/chunk_004.csv`)).toBe(true);
    });
  });

  describe("Timeseries Resampling & Gap Filling Transform", () => {
    it("should correctly parse various time intervals", () => {
      expect(parseTimeInterval("10s").ms).toBe(10000);
      expect(parseTimeInterval("5m").ms).toBe(300000);
      expect(parseTimeInterval("1h").ms).toBe(3600000);
      expect(parseTimeInterval("1d").ms).toBe(86400000);
      expect(parseTimeInterval("1w").ms).toBe(604800000);
      expect(parseTimeInterval("1mo").unit).toBe("mo");
      expect(parseTimeInterval("1y").unit).toBe("y");
    });

    it("should parse timestamps in ISO, unix seconds, and milliseconds", () => {
      expect(parseTimestamp("2026-09-16T12:00:00.000Z")).toBe(
        new Date("2026-09-16T12:00:00.000Z").getTime()
      );
      expect(parseTimestamp(1700000000)).toBe(1700000000000); // unix seconds
      expect(parseTimestamp(1700000000000)).toBe(1700000000000); // unix ms
    });

    it("should resample rows into 1-hour time buckets and compute multi-column aggregations", async () => {
      const rows: Row[] = [
        { time: "2026-01-01T10:05:00Z", cpu: 50, mem: 100 },
        { time: "2026-01-01T10:25:00Z", cpu: 70, mem: 120 },
        { time: "2026-01-01T11:10:00Z", cpu: 30, mem: 80 },
        { time: "2026-01-01T11:40:00Z", cpu: 50, mem: 90 },
      ];

      const pipeline = createPipeline(rows).pipe(
        resampleTimeseries({
          timeCol: "time",
          interval: "1h",
          aggregations: [
            { fn: "avg", column: "cpu", alias: "avg_cpu" },
            { fn: "max", column: "mem", alias: "max_mem" },
            { fn: "count", column: "*", alias: "count" },
          ],
        })
      );

      const result = await pipeline.toArray();
      expect(result).toHaveLength(2);
      expect(result[0]!["avg_cpu"]).toBe(60);
      expect(result[0]!["max_mem"]).toBe(120);
      expect(result[0]!["count"]).toBe(2);

      expect(result[1]!["avg_cpu"]).toBe(40);
      expect(result[1]!["max_mem"]).toBe(90);
      expect(result[1]!["count"]).toBe(2);
    });

    it("should fill missing time gaps with forward fill (ffill)", async () => {
      const rows: Row[] = [
        { time: "2026-01-01T10:00:00Z", temp: 20 },
        // Gap at 11:00:00
        // Gap at 12:00:00
        { time: "2026-01-01T13:00:00Z", temp: 26 },
      ];

      const pipeline = createPipeline(rows).pipe(
        resampleTimeseries({
          timeCol: "time",
          interval: "1h",
          aggregations: [{ fn: "avg", column: "temp", alias: "avg_temp" }],
          fillGaps: "ffill",
        })
      );

      const result = await pipeline.toArray();
      expect(result).toHaveLength(4); // 10:00, 11:00, 12:00, 13:00
      expect(result[0]!["avg_temp"]).toBe(20);
      expect(result[1]!["avg_temp"]).toBe(20); // forward filled
      expect(result[2]!["avg_temp"]).toBe(20); // forward filled
      expect(result[3]!["avg_temp"]).toBe(26);
    });

    it("should fill missing time gaps with linear interpolation", async () => {
      const rows: Row[] = [
        { time: "2026-01-01T10:00:00Z", val: 10 },
        // 11:00 should be 20
        // 12:00 should be 30
        { time: "2026-01-01T13:00:00Z", val: 40 },
      ];

      const pipeline = createPipeline(rows).pipe(
        resampleTimeseries({
          timeCol: "time",
          interval: "1h",
          aggregations: [{ fn: "avg", column: "val", alias: "avg_val" }],
          fillGaps: "linear",
        })
      );

      const result = await pipeline.toArray();
      expect(result).toHaveLength(4);
      expect(result[0]!["avg_val"]).toBe(10);
      expect(result[1]!["avg_val"]).toBe(20);
      expect(result[2]!["avg_val"]).toBe(30);
      expect(result[3]!["avg_val"]).toBe(40);
    });
  });

  describe("CLI Command Integration", () => {
    it("should partition CSV dataset using rowpipe partition CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} partition samples/titanic.csv --by Pclass --out-pattern "${scratchDir}/cli_part/{Pclass}.csv" --limit 50`
      );
      expect(stdout).toContain("Partition complete");
      expect(existsSync(`${scratchDir}/cli_part/1.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/cli_part/2.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/cli_part/3.csv`)).toBe(true);
    });

    it("should split dataset using rowpipe split CLI", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} split samples/titanic.csv --chunk-size 300 --out-pattern "${scratchDir}/cli_split/part_{n:02d}.csv"`
      );
      expect(stdout).toContain("Split complete");
      expect(existsSync(`${scratchDir}/cli_split/part_01.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/cli_split/part_02.csv`)).toBe(true);
      expect(existsSync(`${scratchDir}/cli_split/part_03.csv`)).toBe(true);
    });

    it("should resample timeseries data using rowpipe timeseries CLI", async () => {
      const { stdout } = await execAsync(
        `printf "timestamp,cpu\\n2026-01-01T00:01:00Z,20\\n2026-01-01T00:03:00Z,40\\n2026-01-01T00:12:00Z,80\\n" | node ${cliPath} timeseries - --time-col timestamp --interval 10m --avg cpu --fill-gaps zero --to table`
      );
      expect(stdout).toContain("avg_cpu");
      expect(stdout).toContain("30"); // avg of 20 and 40 in first 10m bucket
      expect(stdout).toContain("80"); // bucket 2
    });
  });
});
