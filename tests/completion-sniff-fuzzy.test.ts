import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { mkdir, rm, writeFile, readFile, stat } from "node:fs/promises";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import {
  sniffFormatFromBuffer,
  sniffFormatFromFile,
  inferFormatFromPath,
  createReader,
} from "../src/readers/index.js";
import { createWriter } from "../src/writers/index.js";
import { findClosestMatch, levenshteinDistance } from "../src/utils/fuzzy.js";
import { installCompletion } from "../src/cli/completion.js";
import { InvalidArgumentError } from "../src/core/errors.js";

const execAsync = promisify(exec);
const TEST_DIR = join(process.cwd(), "scratch_test_step5");
const CLI_PATH = join(process.cwd(), "dist/cli/index.js");

describe("Step 5 DX: Magic Byte Format Sniffing, Did You Mean & Shell Completion Install", () => {
  const originalHome = process.env.HOME;

  beforeEach(async () => {
    await rm(TEST_DIR, { recursive: true, force: true });
    await mkdir(TEST_DIR, { recursive: true });
    process.env.HOME = TEST_DIR;
  });

  afterEach(async () => {
    process.env.HOME = originalHome;
    await rm(TEST_DIR, { recursive: true, force: true });
  });

  describe("Magic Byte Format Sniffing (sniffFormatFromBuffer & sniffFormatFromFile)", () => {
    it("should accurately detect binary format magic bytes", () => {
      // Parquet: PAR1
      const parquetBuf = Buffer.from([0x50, 0x41, 0x52, 0x31, 0x00, 0x00]);
      expect(sniffFormatFromBuffer(parquetBuf)).toBe("parquet");

      // Arrow IPC: ARROW1
      const arrowBuf = Buffer.from("ARROW1\0\0");
      expect(sniffFormatFromBuffer(arrowBuf)).toBe("arrow");

      // Avro: Obj\x01
      const avroBuf = Buffer.from([0x4f, 0x62, 0x6a, 0x01, 0x02, 0x03]);
      expect(sniffFormatFromBuffer(avroBuf)).toBe("avro");

      // SQLite: SQLite format 3\0
      const sqliteBuf = Buffer.from("SQLite format 3\0extra_database_bytes");
      expect(sniffFormatFromBuffer(sqliteBuf)).toBe("sqlite");

      // XLSX: PK\x03\x04
      const xlsxBuf = Buffer.from([0x50, 0x4b, 0x03, 0x04, 0x14, 0x00]);
      expect(sniffFormatFromBuffer(xlsxBuf)).toBe("xlsx");
    });

    it("should detect text formats including XML, JSON, and NDJSON", () => {
      // XML
      expect(sniffFormatFromBuffer(Buffer.from("<?xml version=\"1.0\"?><root></root>"))).toBe("xml");
      expect(sniffFormatFromBuffer(Buffer.from("<dataset><item id=\"1\"/></dataset>"))).toBe("xml");

      // JSON Array
      expect(sniffFormatFromBuffer(Buffer.from("[{\"id\": 1, \"name\": \"test\"}]"))).toBe("json");

      // NDJSON / JSONL
      expect(sniffFormatFromBuffer(Buffer.from("{\"id\": 1}\n{\"id\": 2}\n"))).toBe("ndjson");

      // Single JSON object
      expect(sniffFormatFromBuffer(Buffer.from("{\"key\": \"value\"}"))).toBe("json");

      // TSV
      expect(sniffFormatFromBuffer(Buffer.from("col1\tcol2\tcol3\nval1\tval2\tval3\n"))).toBe("tsv");

      // PSV
      expect(sniffFormatFromBuffer(Buffer.from("col1|col2|col3\nval1|val2|val3\n"))).toBe("psv");
    });

    it("should sniff format from extensionless files on disk", async () => {
      const extLessAvro = join(TEST_DIR, "data_without_ext");
      // Write Avro magic bytes
      await writeFile(extLessAvro, Buffer.from([0x4f, 0x62, 0x6a, 0x01, 0x00, 0x00]));

      expect(sniffFormatFromFile(extLessAvro)).toBe("avro");
      expect(inferFormatFromPath(extLessAvro)).toBe("avro");

      const extLessXml = join(TEST_DIR, "catalog_data");
      await writeFile(extLessXml, "<catalog><book id=\"bk101\"/></catalog>", "utf-8");

      expect(sniffFormatFromFile(extLessXml)).toBe("xml");
      expect(inferFormatFromPath(extLessXml)).toBe("xml");
    });
  });

  describe("Akıllı Öneri ve Hata Kurtarma ('Did You Mean?')", () => {
    it("should find closest matching format names using Levenshtein distance", () => {
      const supported = ["parquet", "arrow", "avro", "xml", "csv", "jsonl", "ndjson"];

      expect(findClosestMatch("parqet", supported)).toBe("parquet");
      expect(findClosestMatch("parquett", supported)).toBe("parquet");
      expect(findClosestMatch("arow", supported)).toBe("arrow");
      expect(findClosestMatch("avr", supported)).toBe("avro");
      expect(findClosestMatch("ndjsn", supported)).toBe("ndjson");
      expect(findClosestMatch("totally_unknown_gibberish", supported)).toBeNull();
    });

    it("should include 'Did you mean' suggestions in createReader error messages", () => {
      expect(() => {
        createReader("sample.csv", { format: "parqet" });
      }).toThrowError(
        /Unsupported input format: "parqet"\. Did you mean "parquet"\? Supported formats are:/
      );
    });

    it("should include 'Did you mean' suggestions in createWriter error messages", () => {
      expect(() => {
        createWriter("output.dat", { format: "ndjsn" });
      }).toThrowError(
        /Unsupported output format: "ndjsn"\. Did you mean "ndjson"\? Supported formats are:/
      );
    });
  });

  describe("Shell Completion Kurulum Ergonomisi (rowpipe completion --install)", () => {
    it("should install completion hook into ~/.zshrc", async () => {
      const res = await installCompletion("zsh");
      expect(res.success).toBe(true);
      expect(res.shell).toBe("zsh");
      expect(res.alreadyInstalled).toBe(false);

      const zshrcContent = await readFile(join(TEST_DIR, ".zshrc"), "utf-8");
      expect(zshrcContent).toContain('eval "$(rowpipe completion zsh)"');

      // Second run should recognize it is already installed
      const res2 = await installCompletion("zsh");
      expect(res2.alreadyInstalled).toBe(true);
    });

    it("should install completion hook into ~/.bashrc", async () => {
      const res = await installCompletion("bash");
      expect(res.success).toBe(true);
      expect(res.shell).toBe("bash");

      const bashrc = join(TEST_DIR, ".bashrc");
      const bashContent = await readFile(bashrc, "utf-8");
      expect(bashContent).toContain('eval "$(rowpipe completion bash)"');
    });

    it("should install fish completion script into ~/.config/fish/completions/rowpipe.fish", async () => {
      const res = await installCompletion("fish");
      expect(res.success).toBe(true);
      expect(res.shell).toBe("fish");

      const fishFile = join(TEST_DIR, ".config", "fish", "completions", "rowpipe.fish");
      const statInfo = await stat(fishFile);
      expect(statInfo.isFile()).toBe(true);

      const fishContent = await readFile(fishFile, "utf-8");
      expect(fishContent).toContain("complete -c rowpipe");
    });

    it("should execute 'rowpipe completion --install' via CLI successfully", async () => {
      const { stdout } = await execAsync(`node "${CLI_PATH}" completion zsh --install`, {
        env: { ...process.env, HOME: TEST_DIR, FORCE_COLOR: "0" },
      });

      expect(stdout).toContain("Autocompletion for zsh successfully installed");
      const zshrcContent = await readFile(join(TEST_DIR, ".zshrc"), "utf-8");
      expect(zshrcContent).toContain('eval "$(rowpipe completion zsh)"');
    });
  });
});
