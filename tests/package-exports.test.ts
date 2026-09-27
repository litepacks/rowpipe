import { describe, it, expect } from "vitest";

describe("Package Subpath Exports & Modular DX", () => {
  it("should import core primitives directly from rowpipe/core", async () => {
    // Dynamic import to test ESM resolution of dist exports
    const core = await import("../dist/core/index.js");
    expect(core).toBeDefined();
    expect(typeof core.Pipeline).toBe("function");
    expect(typeof core.rowsToBatches).toBe("function");
    expect(typeof core.batchesToRows).toBe("function");
    expect(typeof core.RowpipeError).toBe("function");
    expect(typeof core.rowpipe).toBe("object");
    expect(typeof core.rowpipe.from).toBe("function");
  });

  it("should import default and named rowpipe from root index", async () => {
    const root = await import("../dist/index.js");
    expect(root.default).toBeDefined();
    expect(typeof root.default.from).toBe("function");
    expect(typeof root.rowpipe.from).toBe("function");
  });

  it("should import readers directly from rowpipe/readers", async () => {
    const readers = await import("../dist/readers/index.js");
    expect(readers).toBeDefined();
    expect(typeof readers.CSVReader).toBe("function");
    expect(typeof readers.JSONLReader).toBe("function");
    expect(typeof readers.NDJSONReader).toBe("function");
    expect(typeof readers.ArrowReader).toBe("function");
    expect(typeof readers.AvroReader).toBe("function");
    expect(typeof readers.XMLReader).toBe("function");
    expect(typeof readers.createReader).toBe("function");
    expect(typeof readers.sniffFormatFromBuffer).toBe("function");
    expect(typeof readers.sniffFormatFromFile).toBe("function");
  });

  it("should import writers directly from rowpipe/writers", async () => {
    const writers = await import("../dist/writers/index.js");
    expect(writers).toBeDefined();
    expect(typeof writers.CSVWriter).toBe("function");
    expect(typeof writers.JSONLWriter).toBe("function");
    expect(typeof writers.NDJSONWriter).toBe("function");
    expect(typeof writers.ArrowWriter).toBe("function");
    expect(typeof writers.AvroWriter).toBe("function");
    expect(typeof writers.XMLWriter).toBe("function");
    expect(typeof writers.createWriter).toBe("function");
  });

  it("should import transforms directly from rowpipe/transforms", async () => {
    const transforms = await import("../dist/transforms/index.js");
    expect(transforms).toBeDefined();
    expect(typeof transforms.selectColumns).toBe("function");
    expect(typeof transforms.filterRows).toBe("function");
    expect(typeof transforms.castColumns).toBe("function");
  });

  it("should import analytics directly from rowpipe/analytics", async () => {
    const analytics = await import("../dist/analytics/index.js");
    expect(analytics).toBeDefined();
    expect(typeof analytics.DatasetStatsAggregator).toBe("function");
    expect(typeof analytics.SchemaInferenceAggregator).toBe("function");
  });

  it("should import mcp server components directly from rowpipe/mcp", async () => {
    const mcp = await import("../dist/mcp/index.js");
    expect(mcp).toBeDefined();
    expect(typeof mcp.createRowpipeMcpServer).toBe("function");
    expect(typeof mcp.inspectToolDefinition).toBe("object");
  });
});
