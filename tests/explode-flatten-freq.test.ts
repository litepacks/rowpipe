import { describe, it, expect } from "vitest";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import { explodeRows } from "../src/transforms/explode.js";
import { flattenRows } from "../src/transforms/flatten.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { DataBatch, Row } from "../src/core/types.js";

const execAsync = promisify(exec);
const cliPath = join(process.cwd(), "dist/cli/index.js");

describe("Explode, Flatten & Frequency Distribution Suite", () => {
  describe("explodeRows Transform", () => {
    it("should explode delimited string into multiple rows and trim whitespace", async () => {
      const rows: Row[] = [
        { id: 1, title: "Movie A", country: "United States, Germany, France" },
        { id: 2, title: "Movie B", country: "Turkey" },
        { id: 3, title: "Movie C", country: null },
      ];

      const pipeline = createPipeline(rows).pipe(
        explodeRows({ column: "country", delimiter: "," })
      );

      const result = await pipeline.toArray();
      expect(result).toHaveLength(4);
      expect(result[0]).toEqual({ id: 1, title: "Movie A", country: "United States" });
      expect(result[1]).toEqual({ id: 1, title: "Movie A", country: "Germany" });
      expect(result[2]).toEqual({ id: 1, title: "Movie A", country: "France" });
      expect(result[3]).toEqual({ id: 2, title: "Movie B", country: "Turkey" });
    });

    it("should explode JSON array strings and native arrays", async () => {
      const rows: Row[] = [
        { id: 1, tags: '["action", "comedy"]' },
        { id: 2, tags: ["drama", "thriller"] },
      ];

      const pipeline = createPipeline(rows).pipe(explodeRows({ column: "tags" }));
      const result = await pipeline.toArray();

      expect(result).toHaveLength(4);
      expect(result[0]).toEqual({ id: 1, tags: "action" });
      expect(result[1]).toEqual({ id: 1, tags: "comedy" });
      expect(result[2]).toEqual({ id: 2, tags: "drama" });
      expect(result[3]).toEqual({ id: 2, tags: "thriller" });
    });

    it("should preserve null when preserveNullAndEmpty is true", async () => {
      const rows: Row[] = [
        { id: 1, country: "" },
        { id: 2, country: null },
      ];

      const pipeline = createPipeline(rows).pipe(
        explodeRows({ column: "country", preserveNullAndEmpty: true })
      );
      const result = await pipeline.toArray();

      expect(result).toHaveLength(2);
      expect(result[0]).toEqual({ id: 1, country: null });
      expect(result[1]).toEqual({ id: 2, country: null });
    });
  });

  describe("flattenRows Transform", () => {
    it("should flatten nested objects into dot-separated keys", async () => {
      const rows: Row[] = [
        {
          id: 101,
          user: {
            name: "Alice",
            profile: {
              age: 28,
              city: "Istanbul",
            },
          },
          status: "active",
        },
      ];

      const pipeline = createPipeline(rows).pipe(flattenRows({ separator: "." }));
      const result = await pipeline.toArray();

      expect(result).toHaveLength(1);
      expect(result[0]).toEqual({
        id: 101,
        "user.name": "Alice",
        "user.profile.age": 28,
        "user.profile.city": "Istanbul",
        status: "active",
      });
    });

    it("should respect maxDepth option", async () => {
      const rows: Row[] = [
        {
          a: {
            b: {
              c: "deep",
            },
          },
        },
      ];

      const pipeline = createPipeline(rows).pipe(flattenRows({ maxDepth: 2 }));
      const result = await pipeline.toArray();

      expect(result[0]).toEqual({
        "a.b": { c: "deep" },
      });
    });
  });

  describe("CLI Integration & Ergonomics", () => {
    it("should execute rowpipe explode from stdin or file", async () => {
      const { stdout } = await execAsync(
        `printf 'id,country\\n1,"United States, Canada"\\n2,Germany\\n' | node ${cliPath} explode - country --delimiter ","`
      );
      expect(stdout).toContain("United States");
      expect(stdout).toContain("Canada");
      expect(stdout).toContain("Germany");
    });

    it("should execute unified pipeline with --explode flag", async () => {
      const { stdout } = await execAsync(
        `printf "id,genres\n1,Action|Adventure\n2,Sci-Fi\n" | node ${cliPath} - --explode genres --explode-delimiter "|"`
      );
      expect(stdout).toContain("Action");
      expect(stdout).toContain("Adventure");
      expect(stdout).toContain("Sci-Fi");
    });

    it("should execute rowpipe freq command with formatted output", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} freq samples/titanic.csv Sex --top 5`
      );
      expect(stdout).toContain("Frequency Distribution: Sex");
      expect(stdout).toContain("male");
      expect(stdout).toContain("female");
      expect(stdout).toContain("%");
    });

    it("should execute rowpipe freq with --json output", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} freq samples/titanic.csv Survived --json`
      );
      const json = JSON.parse(stdout);
      expect(Array.isArray(json)).toBe(true);
      expect(json[0]).toHaveProperty("Survived");
      expect(json[0]).toHaveProperty("count");
      expect(json[0]).toHaveProperty("percent");
    });

    it("should support positional sort specs (e.g. sort input 'col:desc')", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} sort samples/titanic.csv "Age:desc" | node ${cliPath} limit - 3`
      );
      const lines = stdout.trim().split("\n");
      // 1 header + 3 data rows = 4 lines
      expect(lines.length).toBe(4);
    });

    it("should support positional head and tail row counts", async () => {
      const { stdout: headOut } = await execAsync(
        `node ${cliPath} head samples/titanic.csv 4`
      );
      const headLines = headOut.trim().split("\n");
      // 1 header + 4 data rows = 5 lines
      expect(headLines.length).toBe(5);

      const { stdout: tailOut } = await execAsync(
        `node ${cliPath} tail samples/titanic.csv 3`
      );
      const tailLines = tailOut.trim().split("\n");
      // 1 header + 3 data rows = 4 lines
      expect(tailLines.length).toBe(4);
    });
  });
});
