import { describe, it, expect, vi } from "vitest";
import { formatsCommand, SUPPORTED_FORMATS } from "../src/cli/commands/formats.js";

describe("rowpipe formats command", () => {
  it("should contain all 16 supported formats in the capabilities matrix", () => {
    const formats = SUPPORTED_FORMATS.map((f) => f.format);
    expect(formats).toContain("csv");
    expect(formats).toContain("tsv");
    expect(formats).toContain("psv");
    expect(formats).toContain("json");
    expect(formats).toContain("jsonl");
    expect(formats).toContain("ndjson");
    expect(formats).toContain("arrow");
    expect(formats).toContain("parquet");
    expect(formats).toContain("avro");
    expect(formats).toContain("xml");
    expect(formats).toContain("xlsx");
    expect(formats).toContain("markdown");
    expect(formats).toContain("table");
    expect(formats).toContain("sqlite");
    expect(formats).toContain("postgres");
    expect(formats).toContain("mysql");
  });

  it("should output valid JSON array when --json option is passed", async () => {
    let captured = "";
    const originalWrite = process.stdout.write;
    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      captured += chunk.toString();
      return true;
    }) as any;

    try {
      await formatsCommand({ json: true });
      const parsed = JSON.parse(captured);
      expect(Array.isArray(parsed)).toBe(true);
      expect(parsed.length).toBe(16);
      expect(parsed.find((f: any) => f.format === "arrow").category).toBe("Columnar");
      expect(parsed.find((f: any) => f.format === "avro").category).toBe("Binary");
      expect(parsed.find((f: any) => f.format === "xml").read).toBe("Streaming");
    } finally {
      process.stdout.write = originalWrite;
    }
  });

  it("should output Markdown table when --markdown option is passed", async () => {
    let captured = "";
    const originalWrite = process.stdout.write;
    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      captured += chunk.toString();
      return true;
    }) as any;

    try {
      await formatsCommand({ markdown: true });
      expect(captured).toContain("| Format | Name | Category | Read | Write | Compression | Schema | Nested |");
      expect(captured).toContain("| **arrow** | Apache Arrow IPC / Feather |");
      expect(captured).toContain("| **avro** | Apache Avro (OCF) |");
      expect(captured).toContain("| **xml** | Extensible Markup Language |");
    } finally {
      process.stdout.write = originalWrite;
    }
  });

  it("should filter formats by category when --category is specified", async () => {
    let captured = "";
    const originalWrite = process.stdout.write;
    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      captured += chunk.toString();
      return true;
    }) as any;

    try {
      await formatsCommand({ json: true, category: "columnar" });
      const parsed = JSON.parse(captured);
      expect(parsed.length).toBe(2);
      expect(parsed.map((p: any) => p.format).sort()).toEqual(["arrow", "parquet"].sort());
    } finally {
      process.stdout.write = originalWrite;
    }
  });

  it("should output pretty Unicode table by default", async () => {
    let captured = "";
    const originalWrite = process.stdout.write;
    process.stdout.write = vi.fn((chunk: string | Uint8Array) => {
      captured += chunk.toString();
      return true;
    }) as any;

    try {
      await formatsCommand({});
      expect(captured).toContain("┌");
      expect(captured).toContain("arrow");
      expect(captured).toContain("avro");
      expect(captured).toContain("xml");
      expect(captured).toContain("┘");
    } finally {
      process.stdout.write = originalWrite;
    }
  });
});
