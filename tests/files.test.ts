import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { promises as fsPromises } from "node:fs";
import * as path from "node:path";
import * as os from "node:os";
import {
  FileSystemReader,
  createGlobMatcher,
  inferMimeAndCategory,
  computeFileHash,
  computeFastFileHash,
  createPipeline,
  filterRows,
  selectColumns,
  JSONLWriter,
  diffRows,
  computeDiff,
} from "../src/index.js";

describe("FileSystem as Streaming Data Source", () => {
  let tmpDir: string;

  beforeEach(async () => {
    tmpDir = await fsPromises.mkdtemp(path.join(os.tmpdir(), "rowpipe-files-test-"));
  });

  afterEach(async () => {
    try {
      await fsPromises.rm(tmpDir, { recursive: true, force: true });
    } catch {
      // Ignore cleanup error
    }
  });

  describe("Glob Matching", () => {
    it("should match simple wildcards and extensions", () => {
      const matcher = createGlobMatcher("*.ts");
      expect(matcher("file.ts")).toBe(true);
      expect(matcher("src/index.ts")).toBe(true);
      expect(matcher("file.js")).toBe(false);
    });

    it("should match recursive wildcards", () => {
      const matcher = createGlobMatcher("**/*.json");
      expect(matcher("data.json")).toBe(true);
      expect(matcher("configs/app.json")).toBe(true);
      expect(matcher("configs/nested/deep/app.json")).toBe(true);
      expect(matcher("data.csv")).toBe(false);
    });

    it("should match brace expansions", () => {
      const matcher = createGlobMatcher("*.{png,jpg,webp}");
      expect(matcher("avatar.png")).toBe(true);
      expect(matcher("photo.jpg")).toBe(true);
      expect(matcher("banner.webp")).toBe(true);
      expect(matcher("doc.pdf")).toBe(false);
    });

    it("should match multiple comma-separated patterns", () => {
      const matcher = createGlobMatcher("*.csv, *.tsv, data/**");
      expect(matcher("sales.csv")).toBe(true);
      expect(matcher("reports.tsv")).toBe(true);
      expect(matcher("data/users.jsonl")).toBe(true);
      expect(matcher("logs/access.log")).toBe(false);
    });
  });

  describe("MIME Detection & Categories", () => {
    it("should infer MIME types and categories correctly", () => {
      expect(inferMimeAndCategory("avatar.png")).toEqual({
        mime: "image/png",
        category: "image",
      });
      expect(inferMimeAndCategory("sales.csv")).toEqual({
        mime: "text/csv",
        category: "data",
      });
      expect(inferMimeAndCategory("dataset.parquet")).toEqual({
        mime: "application/vnd.apache.parquet",
        category: "data",
      });
      expect(inferMimeAndCategory("report.pdf")).toEqual({
        mime: "application/pdf",
        category: "document",
      });
      expect(inferMimeAndCategory("archive.zip")).toEqual({
        mime: "application/zip",
        category: "archive",
      });
      expect(inferMimeAndCategory("app.ts")).toEqual({
        mime: "text/typescript",
        category: "code",
      });
      expect(inferMimeAndCategory("unknown.xyz123")).toEqual({
        mime: "application/octet-stream",
        category: "other",
      });
    });
  });

  describe("Streaming Hashes", () => {
    it("should compute accurate SHA-256 and MD5 hashes", async () => {
      const filePath = path.join(tmpDir, "sample.txt");
      await fsPromises.writeFile(filePath, "Hello Rowpipe Filesystem!");

      const sha256 = await computeFileHash(filePath, "sha256");
      expect(sha256).toBe("3ec752e145669c4b3fb1b9e0a68f749d4d45830b1d5520d4df424230e899bf0b");

      const md5 = await computeFileHash(filePath, "md5");
      expect(md5).toBe("647f59090a7b20e94134267f90c23ac2");
    });



    it("should compute fast non-cryptographic fingerprints", async () => {
      const filePath = path.join(tmpDir, "sample.txt");
      await fsPromises.writeFile(filePath, "Fast fingerprint content");

      const fastHash = await computeFastFileHash(filePath);
      expect(fastHash.startsWith("fast:")).toBe(true);
    });
  });

  describe("Directory Traversal & Record Generation", () => {
    it("should stream records for single file", async () => {
      const filePath = path.join(tmpDir, "hello.csv");
      await fsPromises.writeFile(filePath, "id,name\n1,Alice");

      const reader = new FileSystemReader(filePath);
      const rows: Record<string, unknown>[] = [];

      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows.length).toBe(1);
      const row = rows[0]!;
      expect(row["name"]).toBe("hello.csv");
      expect(row["basename"]).toBe("hello");
      expect(row["extension"]).toBe("csv");
      expect(row["is_file"]).toBe(true);
      expect(row["is_directory"]).toBe(false);
      expect(Number(row["size"])).toBeGreaterThan(0);
    });

    it("should recursively traverse nested directory trees", async () => {
      // Structure:
      // tmpDir/
      //   root.txt
      //   sub1/
      //     child1.csv
      //     sub2/
      //       child2.json
      await fsPromises.writeFile(path.join(tmpDir, "root.txt"), "root");
      await fsPromises.mkdir(path.join(tmpDir, "sub1", "sub2"), { recursive: true });
      await fsPromises.writeFile(path.join(tmpDir, "sub1", "child1.csv"), "col1\nval1");
      await fsPromises.writeFile(path.join(tmpDir, "sub1", "sub2", "child2.json"), '{"a":1}');

      const reader = new FileSystemReader({ root: tmpDir, recursive: true });
      const rows: Record<string, unknown>[] = [];

      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows.length).toBe(3);
      const relPaths = rows.map((r) => r["relative_path"]).sort();
      expect(relPaths).toEqual(["root.txt", "sub1/child1.csv", "sub1/sub2/child2.json"]);
    });

    it("should respect --max-depth", async () => {
      await fsPromises.writeFile(path.join(tmpDir, "d0.txt"), "d0");
      await fsPromises.mkdir(path.join(tmpDir, "level1", "level2"), { recursive: true });
      await fsPromises.writeFile(path.join(tmpDir, "level1", "d1.txt"), "d1");
      await fsPromises.writeFile(path.join(tmpDir, "level1", "level2", "d2.txt"), "d2");

      const reader = new FileSystemReader({ root: tmpDir, recursive: true, maxDepth: 1 });
      const rows: Record<string, unknown>[] = [];

      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      const relPaths = rows.map((r) => r["relative_path"]).sort();
      expect(relPaths).toEqual(["d0.txt", "level1/d1.txt"]);
    });

    it("should include/exclude files using glob patterns", async () => {
      await fsPromises.mkdir(path.join(tmpDir, "src"), { recursive: true });
      await fsPromises.mkdir(path.join(tmpDir, "dist"), { recursive: true });
      await fsPromises.writeFile(path.join(tmpDir, "src", "index.ts"), "ts");
      await fsPromises.writeFile(path.join(tmpDir, "src", "styles.css"), "css");
      await fsPromises.writeFile(path.join(tmpDir, "dist", "index.js"), "js");

      const reader = new FileSystemReader({
        root: tmpDir,
        include: ["**/*.ts", "**/*.js"],
        exclude: ["dist/**"],
      });

      const rows: Record<string, unknown>[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows.length).toBe(1);
      expect(rows[0]!["relative_path"]).toBe("src/index.ts");
    });

    it("should handle hidden files according to --hidden flag", async () => {
      await fsPromises.writeFile(path.join(tmpDir, "visible.txt"), "visible");
      await fsPromises.writeFile(path.join(tmpDir, ".hidden.txt"), "hidden");
      await fsPromises.mkdir(path.join(tmpDir, ".git"), { recursive: true });
      await fsPromises.writeFile(path.join(tmpDir, ".git", "config"), "config");

      // Default (hidden = false)
      const readerWithoutHidden = new FileSystemReader({ root: tmpDir, hidden: false });
      const rowsWithoutHidden: Record<string, unknown>[] = [];
      for await (const batch of readerWithoutHidden.read()) {
        rowsWithoutHidden.push(...batch.rows);
      }
      expect(rowsWithoutHidden.length).toBe(1);
      expect(rowsWithoutHidden[0]!["name"]).toBe("visible.txt");

      // With hidden = true
      const readerWithHidden = new FileSystemReader({ root: tmpDir, hidden: true });
      const rowsWithHidden: Record<string, unknown>[] = [];
      for await (const batch of readerWithHidden.read()) {
        rowsWithHidden.push(...batch.rows);
      }
      expect(rowsWithHidden.length).toBe(3);
    });

    it("should handle special characters, unicode, spaces, and emoji filenames", async () => {
      const specialNames = ["file with spaces.txt", "türkçe_şçğüöı.csv", "🚀_rocket_data.jsonl"];
      for (const name of specialNames) {
        await fsPromises.writeFile(path.join(tmpDir, name), "data");
      }

      const reader = new FileSystemReader(tmpDir);
      const rows: Record<string, unknown>[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      expect(rows.length).toBe(3);
      const names = rows.map((r) => r["name"]).sort();
      expect(names).toEqual(specialNames.sort());
    });

    it("should support record types: files, directories, symlinks, all", async () => {
      await fsPromises.mkdir(path.join(tmpDir, "subfolder"));
      await fsPromises.writeFile(path.join(tmpDir, "file.txt"), "txt");

      const allReader = new FileSystemReader({ root: tmpDir, type: "all" });
      const allRows: Record<string, unknown>[] = [];
      for await (const batch of allReader.read()) {
        allRows.push(...batch.rows);
      }

      const types = allRows.map((r) => r["type"]).sort();
      expect(types).toContain("file");
      expect(types).toContain("directory");
    });
  });

  describe("Symlink Safety & Cycle Prevention", () => {
    it("should safely handle directory symlinks and prevent recursive cycles", async () => {
      const realDir = path.join(tmpDir, "real");
      await fsPromises.mkdir(realDir);
      await fsPromises.writeFile(path.join(realDir, "inner.txt"), "hello");

      // Create a symlink to realDir inside tmpDir
      const symlinkDir = path.join(tmpDir, "link_to_real");
      await fsPromises.symlink(realDir, symlinkDir);

      // Create a recursive cycle inside realDir pointing back to tmpDir
      const cycleLink = path.join(realDir, "cycle_back");
      await fsPromises.symlink(tmpDir, cycleLink);

      const reader = new FileSystemReader({
        root: tmpDir,
        followSymlinks: true,
        recursive: true,
      });

      const rows: Record<string, unknown>[] = [];
      for await (const batch of reader.read()) {
        rows.push(...batch.rows);
      }

      // Should find the file without infinite loop
      expect(rows.some((r) => r["name"] === "inner.txt")).toBe(true);
    });
  });

  describe("Pipeline Composition (Filter, Select, Stats)", () => {
    it("should filter and select file stream seamlessly", async () => {
      await fsPromises.writeFile(path.join(tmpDir, "small.txt"), "123");
      await fsPromises.writeFile(path.join(tmpDir, "big.txt"), "1234567890".repeat(50));
      await fsPromises.writeFile(path.join(tmpDir, "code.ts"), "console.log('hi');");

      const reader = new FileSystemReader({ root: tmpDir, mime: true });
      const pipeline = createPipeline(reader)
        .pipe(filterRows("size > 10"))
        .pipe(selectColumns(["relative_path", "size", "category"]));

      const resultRows: Record<string, unknown>[] = [];
      for await (const batch of pipeline.batches()) {
        resultRows.push(...batch.rows);
      }

      expect(resultRows.length).toBe(2);
      expect(resultRows.every((r) => Number(r["size"]) > 10)).toBe(true);
    });
  });

  describe("Snapshots & Diff Engine Integration", () => {
    it("should produce deterministic snapshots and diff them accurately", async () => {
      const dirA = path.join(tmpDir, "dirA");
      const dirB = path.join(tmpDir, "dirB");
      await fsPromises.mkdir(dirA);
      await fsPromises.mkdir(dirB);

      // Same unchanged file
      await fsPromises.writeFile(path.join(dirA, "common.txt"), "same content");
      await fsPromises.writeFile(path.join(dirB, "common.txt"), "same content");

      // Modified file (different content -> different hash & size)
      await fsPromises.writeFile(path.join(dirA, "modified.csv"), "v1,old");
      await fsPromises.writeFile(path.join(dirB, "modified.csv"), "v2,new,expanded_row");

      // Deleted file in B
      await fsPromises.writeFile(path.join(dirA, "deleted.txt"), "bye");

      // Added file in B
      await fsPromises.writeFile(path.join(dirB, "added.txt"), "hello new");

      // Generate snapshots as JSONL files
      const snapAPath = path.join(tmpDir, "snapA.jsonl");
      const snapBPath = path.join(tmpDir, "snapB.jsonl");

      const readerA = new FileSystemReader({ root: dirA, hash: "sha256", deterministic: true });
      const writerA = new JSONLWriter(snapAPath);
      await createPipeline(readerA).to(writerA);

      const readerB = new FileSystemReader({ root: dirB, hash: "sha256", deterministic: true });
      const writerB = new JSONLWriter(snapBPath);
      await createPipeline(readerB).to(writerB);

      // Diff the two snapshots with Rowpipe diff engine
      const summary = await computeDiff({
        leftPath: snapAPath,
        rightPath: snapBPath,
        keys: ["relative_path"],
        ignore: ["path", "directory", "modified_at", "accessed_at", "created_at"],
      });


      expect(summary.rows.added).toBe(1); // added.txt
      expect(summary.rows.removed).toBe(1); // deleted.txt
      expect(summary.rows.changed).toBe(1); // modified.csv
      expect(summary.rows.unchanged).toBe(1); // common.txt
      expect(summary.columns["hash"]?.changed).toBe(1);
    });

    it("should diff two directories directly using diffRows stream", async () => {
      const dirA = path.join(tmpDir, "v1");
      const dirB = path.join(tmpDir, "v2");
      await fsPromises.mkdir(dirA);
      await fsPromises.mkdir(dirB);

      await fsPromises.writeFile(path.join(dirA, "app.ts"), "console.log(1)");
      await fsPromises.writeFile(path.join(dirB, "app.ts"), "console.log(2)");

      const events: string[] = [];
      for await (const event of diffRows({
        leftPath: dirA,
        rightPath: dirB,
        fromLeft: "files",
        fromRight: "files",
        keys: ["relative_path"],
        ignore: ["modified_at", "accessed_at", "created_at"],
      })) {
        events.push(event.type);
      }

      expect(events).toContain("changed");
    });
  });

  describe("Bounded Memory Scaling on Large Trees", () => {
    it("should stream 2,000 files in batches without unbounded memory accumulation", async () => {
      const largeTree = path.join(tmpDir, "large_tree");
      await fsPromises.mkdir(largeTree);

      // Create 20 subdirectories with 100 files each = 2,000 files
      for (let d = 0; d < 20; d++) {
        const sub = path.join(largeTree, `dir_${d}`);
        await fsPromises.mkdir(sub);
        for (let f = 0; f < 100; f++) {
          await fsPromises.writeFile(path.join(sub, `file_${f}.txt`), `data_${d}_${f}`);
        }
      }

      const reader = new FileSystemReader({
        root: largeTree,
        batchSize: 250,
      });

      let totalCount = 0;
      let batchCount = 0;

      for await (const batch of reader.read()) {
        totalCount += batch.rows.length;
        batchCount += 1;
        expect(batch.rows.length).toBeLessThanOrEqual(250);
      }

      expect(totalCount).toBe(2000);
      expect(batchCount).toBe(8);
    }, 60000);
  });
});
