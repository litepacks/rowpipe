import { describe, it, expect, vi } from "vitest";
import { TerminalViewer } from "../src/ui/viewer.js";
import { createPipeline } from "../src/core/pipeline.js";

describe("Interactive Terminal Data Viewer (TerminalViewer)", () => {
  const sampleData = [
    { id: 1, name: "Alice", score: 95.5, active: true },
    { id: 2, name: "Bob", score: 82.0, active: false },
    { id: 3, name: "Charlie", score: 88.4, active: true },
    { id: 4, name: "Diana", score: 91.0, active: false },
    { id: 5, name: "Evan", score: 76.2, active: true },
  ];

  it("loads and buffers streaming rows on demand", async () => {
    async function* makeBatches() {
      for (let i = 0; i < sampleData.length; i++) {
        yield { rows: [sampleData[i]!], offset: i };
      }
    }
    const viewer = new TerminalViewer(makeBatches(), {
      initialRows: 3,
      interactive: false,
    });

    // Load initial 3 rows
    await viewer.loadRows(3);
    expect((viewer as any).loadedRows).toHaveLength(3);
    expect((viewer as any).columns).toEqual(["id", "name", "score", "active"]);

    // Load remaining rows
    await viewer.loadRows(10);
    expect((viewer as any).loadedRows).toHaveLength(5);
  });

  it("searches and filters rows by query", async () => {
    const pipeline = createPipeline(sampleData);
    const viewer = new TerminalViewer(pipeline.batches(), { interactive: false });
    await viewer.loadRows(10);

    // Search for "Alice"
    (viewer as any).applySearch("Alice");
    expect((viewer as any).matchingRowIndices).toEqual([0]);

    // Search for "true" (active status)
    (viewer as any).applySearch("true");
    expect((viewer as any).matchingRowIndices).toEqual([0, 2, 4]);

    // Clear search
    (viewer as any).applySearch("");
    expect((viewer as any).matchingRowIndices).toBeNull();
  });

  it("sorts rows ascending and descending by column", async () => {
    const pipeline = createPipeline(sampleData);
    const viewer = new TerminalViewer(pipeline.batches(), { interactive: false });
    await viewer.loadRows(10);

    // Sort by score Ascending
    (viewer as any).applySort("score");
    expect((viewer as any).sortColumn).toBe("score");
    expect((viewer as any).sortAsc).toBe(true);

    const sortedAscIndices: number[] = (viewer as any).sortedRowIndices;
    const scoresAsc = sortedAscIndices.map((i) => sampleData[i]!.score);
    expect(scoresAsc).toEqual([76.2, 82.0, 88.4, 91.0, 95.5]);

    // Sort by score Descending
    (viewer as any).applySort("score");
    expect((viewer as any).sortAsc).toBe(false);
    const sortedDescIndices: number[] = (viewer as any).sortedRowIndices;
    const scoresDesc = sortedDescIndices.map((i) => sampleData[i]!.score);
    expect(scoresDesc).toEqual([95.5, 91.0, 88.4, 82.0, 76.2]);

    // Clear sort
    (viewer as any).applySort("score");
    expect((viewer as any).sortColumn).toBeNull();
    expect((viewer as any).sortedRowIndices).toBeNull();
  });

  it("calculates accurate column widths with auto-fit", async () => {
    const pipeline = createPipeline([
      { short: "x", description: "This is a much longer column value for testing width calculation" },
    ]);
    const viewer = new TerminalViewer(pipeline.batches(), { interactive: false });
    await viewer.loadRows(10);

    const widths = (viewer as any).calculateColumnWidths();
    expect(widths.short).toBeGreaterThanOrEqual(5);
    expect(widths.description).toBeGreaterThanOrEqual(11);
  });

  it("renders non-interactive output table without errors", async () => {
    const pipeline = createPipeline(sampleData);
    const viewer = new TerminalViewer(pipeline.batches(), { interactive: false });
    await viewer.loadRows(10);

    const stdoutSpy = vi.spyOn(process.stdout, "write").mockImplementation(() => true);

    viewer.renderNonInteractive(5);
    expect(stdoutSpy).toHaveBeenCalled();

    stdoutSpy.mockRestore();
  });
});
