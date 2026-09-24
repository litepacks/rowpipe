import { describe, it, expect } from "vitest";
import { exec } from "node:child_process";
import { promisify } from "node:util";
import { join } from "node:path";
import { renderBarChart, renderHistogram, renderSparkline } from "../src/ui/chart.js";
import { formatTable, TableWriter } from "../src/writers/table.js";
import { createWriter } from "../src/writers/index.js";
import {
  generateCompletion,
  generateZshCompletion,
  generateBashCompletion,
  generateFishCompletion,
} from "../src/cli/completion.js";
import { createPipeline } from "../src/core/pipeline.js";
import type { Row } from "../src/core/types.js";

const execAsync = promisify(exec);
const cliPath = join(process.cwd(), "dist/cli/index.js");

describe("Terminal Charts, Tables & Shell Completion Suite", () => {
  describe("Terminal Charts & Histograms (renderBarChart, renderHistogram, renderSparkline)", () => {
    it("should render horizontal bar chart with fractional block precision", () => {
      const items = [
        { label: "USA", value: 100, percent: "50%" },
        { label: "UK", value: 50, percent: "25%" },
        { label: "Germany", value: 25, percent: "12.5%" },
      ];

      const chart = renderBarChart(items, { maxWidth: 20 });
      expect(chart).toContain("USA");
      expect(chart).toContain("UK");
      expect(chart).toContain("Germany");
      expect(chart).toContain("█");
      expect(chart).toContain("50%");
      expect(chart).toContain("25%");
    });

    it("should handle empty items array gracefully", () => {
      expect(renderBarChart([])).toBe("");
    });

    it("should compute frequency bins and render numeric histogram", () => {
      const values = [1, 2, 2, 3, 5, 8, 9, 10, 15, 20, 22, 25, 30, 45, 50];
      const histogram = renderHistogram(values, { bins: 5, width: 20 });

      expect(histogram).toContain("1.0 - ");
      expect(histogram).toContain("%");
      expect(histogram).toContain("█");
    });

    it("should render sparklines for sequences of numbers", () => {
      const sparkline = renderSparkline([1, 3, 5, 8, 10, 7, 4, 2]);
      expect(sparkline.length).toBe(8);
      expect(typeof sparkline).toBe("string");
      expect(sparkline).toContain("█");
    });
  });

  describe("Pretty Grid Table Formatter (formatTable & TableWriter)", () => {
    const sampleRows: Row[] = [
      { id: 1, name: "Alice", score: 95.5, country: "Turkey" },
      { id: 2, name: "Bob", score: 82.0, country: "Germany" },
      { id: 3, name: "Charlie", score: 88.5, country: "United States" },
    ];

    it("should format rows with Unicode box-drawing borders", () => {
      const table = formatTable(sampleRows, { style: "unicode" });
      expect(table).toContain("┌");
      expect(table).toContain("┐");
      expect(table).toContain("└");
      expect(table).toContain("┘");
      expect(table).toContain("│");
      expect(table).toContain("Alice");
      expect(table).toContain("Turkey");
    });

    it("should format rows with ASCII style borders", () => {
      const table = formatTable(sampleRows, { style: "ascii" });
      expect(table).toContain("+");
      expect(table).toContain("|");
      expect(table).toContain("Bob");
      expect(table).toContain("Germany");
    });

    it("should format rows with Compact style", () => {
      const table = formatTable(sampleRows, { style: "compact" });
      expect(table).toContain("Charlie");
      expect(table).toContain("United States");
      expect(table).not.toContain("┌");
    });

    it("should truncate long cell values according to maxColWidth", () => {
      const longRows: Row[] = [
        { id: 1, desc: "This is an extraordinarily long text that should definitely be truncated." },
      ];
      const table = formatTable(longRows, { maxColWidth: 15 });
      expect(table).toContain("…");
    });

    it("should stream and render through TableWriter adapter", async () => {
      let output = "";
      const streamMock = {
        write: (chunk: string) => {
          output += chunk;
          return true;
        },
      } as any;

      const writer = new TableWriter(streamMock, { style: "unicode" });
      const pipeline = createPipeline(sampleRows);
      await writer.write(pipeline.batches());
      await writer.close();

      expect(output).toContain("┌");
      expect(output).toContain("Alice");
      expect(output).toContain("┘");
    });

    it("should be instantiable via createWriter with format 'table'", () => {
      const writer = createWriter("-", { format: "table" });
      expect(writer).toBeInstanceOf(TableWriter);
    });
  });

  describe("Shell Autocompletion (generateCompletion)", () => {
    it("should generate zsh completion script with all subcommands", () => {
      const script = generateZshCompletion();
      expect(script).toContain("#compdef rowpipe");
      expect(script).toContain("_rowpipe()");
      expect(script).toContain("plot:Render horizontal ASCII/Unicode bar charts");
      expect(script).toContain("table:Render dataset as pretty terminal grid table");
      expect(script).toContain("completion:Generate shell autocompletion script");
    });

    it("should generate bash completion script", () => {
      const script = generateBashCompletion();
      expect(script).toContain("_rowpipe_completions()");
      expect(script).toContain("complete -F _rowpipe_completions rowpipe");
      expect(script).toContain("plot");
      expect(script).toContain("table");
      expect(script).toContain("completion");
    });

    it("should generate fish completion script", () => {
      const script = generateFishCompletion();
      expect(script).toContain("complete -c rowpipe");
      expect(script).toContain("fish");
    });

    it("should dispatch completion generation by shell name", () => {
      expect(generateCompletion("zsh")).toContain("#compdef");
      expect(generateCompletion("bash")).toContain("complete -F");
      expect(generateCompletion("fish")).toContain("complete -c rowpipe");
      expect(() => generateCompletion("powershell")).toThrow("Unsupported shell");
    });
  });

  describe("CLI Command Integration (freq --chart, plot, table, completion)", () => {
    it("should execute rowpipe freq with --chart flag", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} freq samples/titanic.csv Sex --chart --top 2`
      );
      expect(stdout).toContain("Frequency Chart: Sex");
      expect(stdout).toContain("male");
      expect(stdout).toContain("female");
      expect(stdout).toContain("█");
    });

    it("should execute rowpipe plot for categorical column", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} plot samples/titanic.csv Sex --top 2`
      );
      expect(stdout).toContain("Frequency Chart: Sex");
      expect(stdout).toContain("male");
      expect(stdout).toContain("female");
      expect(stdout).toContain("█");
    });

    it("should execute rowpipe plot for numeric column with histogram bins", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} plot samples/titanic.csv Age --bins 5 --numeric`
      );
      expect(stdout).toContain("Histogram: Age");
      expect(stdout).toContain("bins");
      expect(stdout).toContain("█");
    });

    it("should execute rowpipe plot with two columns (label and value)", async () => {
      const { stdout } = await execAsync(
        `printf "dept,sales\\nEngineering,50\\nMarketing,30\\nSales,90\\n" | node ${cliPath} plot - dept sales`
      );
      expect(stdout).toContain("Chart: dept vs sales");
      expect(stdout).toContain("Sales");
      expect(stdout).toContain("Engineering");
      expect(stdout).toContain("Marketing");
      expect(stdout).toContain("█");
    });

    it("should execute rowpipe table to format dataset with Unicode borders", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} table samples/titanic.csv --limit 3`
      );
      expect(stdout).toContain("┌");
      expect(stdout).toContain("│");
      expect(stdout).toContain("└");
      expect(stdout).toContain("PassengerId");
      expect(stdout).toContain("Survived");
    });

    it("should execute pipeline with --to table output format", async () => {
      const { stdout } = await execAsync(
        `node ${cliPath} samples/titanic.csv --limit 2 --to table`
      );
      expect(stdout).toContain("┌");
      expect(stdout).toContain("│");
      expect(stdout).toContain("└");
    });

    it("should output zsh completion script via rowpipe completion zsh", async () => {
      const { stdout } = await execAsync(`node ${cliPath} completion zsh`);
      expect(stdout).toContain("#compdef rowpipe");
      expect(stdout).toContain("_rowpipe");
    });

    it("should output bash completion script via rowpipe completion bash", async () => {
      const { stdout } = await execAsync(`node ${cliPath} completion bash`);
      expect(stdout).toContain("_rowpipe_completions");
    });
  });
});
