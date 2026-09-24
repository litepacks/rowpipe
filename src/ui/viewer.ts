import { type Readable } from "node:stream";
import type { DataBatch, DataStream, Row, TabularReader } from "../core/types.js";
import {
  ANSI,
  bgBlue,
  bgGray,
  bold,
  cyan,
  dim,
  gray,
  green,
  inverse,
  magenta,
  red,
  stripAnsi,
  truncateVisible,
  yellow,
} from "./colors.js";
import { formatNumber } from "../utils/formatting.js";

export interface TerminalViewerOptions {
  title?: string;
  filePath?: string;
  maxBufferRows?: number;
  initialRows?: number;
  interactive?: boolean;
}

/**
 * Interactive Terminal Data Viewer for streaming tabular datasets.
 * Features bounded memory sliding windows, keyboard navigation, column browsing,
 * live in-memory searching, sorting, and seamless terminal restoration.
 */
export class TerminalViewer {
  private streamIterator: AsyncIterator<DataBatch>;
  private isStreamExhausted = false;
  private loadedRows: Row[] = [];
  private columns: string[] = [];
  private options: TerminalViewerOptions;

  // Navigation state
  private cursorRow = 0;
  private scrollRow = 0;
  private cursorCol = 0;
  private scrollCol = 0;

  // Interactive modes
  private mode: "normal" | "search" | "help" | "jump" = "normal";
  private searchQuery = "";
  private searchInput = "";
  private jumpInput = "";
  private matchingRowIndices: number[] | null = null;
  private activeMatchIndex = -1;

  // Sorting
  private sortColumn: string | null = null;
  private sortAsc = true;
  private sortedRowIndices: number[] | null = null;

  // Display configuration
  private autoFitCols = true;
  private termRows = 24;
  private termCols = 80;
  private isRunning = false;
  private stdinRawSet = false;

  constructor(
    source: DataStream | TabularReader,
    options: TerminalViewerOptions = {}
  ) {
    this.options = {
      maxBufferRows: 25000,
      initialRows: 500,
      ...options,
    };

    const stream =
      "read" in source && typeof source.read === "function"
        ? source.read()
        : (source as DataStream);

    this.streamIterator = stream[Symbol.asyncIterator]();
  }

  /**
   * Loads at least `targetCount` rows into memory buffer from stream.
   */
  public async loadRows(targetCount: number): Promise<void> {
    while (!this.isStreamExhausted && this.loadedRows.length < targetCount) {
      const result = await this.streamIterator.next();
      if (result.done) {
        this.isStreamExhausted = true;
        break;
      }
      const batch: DataBatch = result.value;
      if (batch && Array.isArray(batch.rows)) {
        for (const row of batch.rows) {
          this.loadedRows.push(row);
          // Discover new column names dynamically
          for (const key of Object.keys(row)) {
            if (!this.columns.includes(key)) {
              this.columns.push(key);
            }
          }
        }
      }
    }
  }

  /**
   * Starts the interactive terminal viewer loop.
   */
  public async run(): Promise<void> {
    // Initial data load
    await this.loadRows(this.options.initialRows || 200);

    // Non-interactive fallback (e.g. CI / piped output)
    if (!process.stdin.isTTY || this.options.interactive === false) {
      this.renderNonInteractive();
      return;
    }

    this.isRunning = true;
    this.updateDimensions();

    // Setup terminal
    process.stdout.write(ANSI.enterAltScreen);
    process.stdout.write(ANSI.hideCursor);

    if (process.stdin.isTTY) {
      process.stdin.setRawMode(true);
      this.stdinRawSet = true;
      process.stdin.resume();
      process.stdin.setEncoding("utf8");
    }

    const onResize = () => {
      this.updateDimensions();
      this.render();
    };
    process.stdout.on("resize", onResize);

    const onData = async (data: string) => {
      if (!this.isRunning) return;
      await this.handleInput(data);
      this.render();
    };
    process.stdin.on("data", onData);

    // Initial render
    this.render();

    // Keep running until exit requested
    return new Promise<void>((resolve) => {
      const cleanup = () => {
        this.isRunning = false;
        process.stdout.off("resize", onResize);
        process.stdin.off("data", onData);

        if (this.stdinRawSet && process.stdin.isTTY) {
          process.stdin.setRawMode(false);
          process.stdin.pause();
        }

        process.stdout.write(ANSI.showCursor);
        process.stdout.write(ANSI.exitAltScreen);
        resolve();
      };

      this.exitCleanup = cleanup;
    });
  }

  private exitCleanup?: () => void;

  private exit(): void {
    if (this.exitCleanup) {
      this.exitCleanup();
    }
  }

  private updateDimensions(): void {
    this.termRows = process.stdout.rows || 24;
    this.termCols = process.stdout.columns || 80;
  }

  private getEffectiveRowIndices(): number[] {
    if (this.matchingRowIndices !== null) {
      return this.matchingRowIndices;
    }
    if (this.sortedRowIndices !== null) {
      return this.sortedRowIndices;
    }
    const indices: number[] = [];
    for (let i = 0; i < this.loadedRows.length; i++) {
      indices.push(i);
    }
    return indices;
  }

  private applySearch(query: string): void {
    this.searchQuery = query.trim();
    if (!this.searchQuery) {
      this.matchingRowIndices = null;
      this.activeMatchIndex = -1;
      return;
    }

    const lowerQuery = this.searchQuery.toLowerCase();
    const matches: number[] = [];
    const sourceIndices = this.sortedRowIndices || Array.from({ length: this.loadedRows.length }, (_, i) => i);

    for (const idx of sourceIndices) {
      const row = this.loadedRows[idx];
      if (!row) continue;
      const rowText = Object.values(row).map(String).join(" ").toLowerCase();
      if (rowText.includes(lowerQuery)) {
        matches.push(idx);
      }
    }

    this.matchingRowIndices = matches;
    this.activeMatchIndex = matches.length > 0 ? 0 : -1;
    this.cursorRow = 0;
    this.scrollRow = 0;
  }

  private applySort(column: string): void {
    if (this.sortColumn === column) {
      if (this.sortAsc) {
        this.sortAsc = false;
      } else {
        // Clear sort
        this.sortColumn = null;
        this.sortedRowIndices = null;
        if (this.searchQuery) {
          this.applySearch(this.searchQuery);
        }
        return;
      }
    } else {
      this.sortColumn = column;
      this.sortAsc = true;
    }

    const indices = Array.from({ length: this.loadedRows.length }, (_, i) => i);
    const asc = this.sortAsc;

    indices.sort((a, b) => {
      const valA = this.loadedRows[a]?.[column];
      const valB = this.loadedRows[b]?.[column];

      if (valA === valB) return 0;
      if (valA === null || valA === undefined) return 1;
      if (valB === null || valB === undefined) return -1;

      if (typeof valA === "number" && typeof valB === "number") {
        return asc ? valA - valB : valB - valA;
      }

      const strA = String(valA);
      const strB = String(valB);
      return asc ? strA.localeCompare(strB) : strB.localeCompare(strA);
    });

    this.sortedRowIndices = indices;
    if (this.searchQuery) {
      this.applySearch(this.searchQuery);
    }
  }

  private async handleInput(key: string): Promise<void> {
    // -------------------------------------------------------------
    // 1. Search Mode Input
    // -------------------------------------------------------------
    if (this.mode === "search") {
      if (key === "\r" || key === "\n") {
        // Submit search
        this.applySearch(this.searchInput);
        this.mode = "normal";
      } else if (key === "\x1b" || key === "\x03") {
        // Escape / Cancel
        this.mode = "normal";
        this.searchInput = this.searchQuery;
      } else if (key === "\x7f" || key === "\b" || key === "\x08") {
        // Backspace
        this.searchInput = this.searchInput.slice(0, -1);
      } else if (key.length === 1 && key >= " ") {
        this.searchInput += key;
      }
      return;
    }

    // -------------------------------------------------------------
    // 2. Jump to Row Mode Input
    // -------------------------------------------------------------
    if (this.mode === "jump") {
      if (key === "\r" || key === "\n") {
        const targetRow = Number.parseInt(this.jumpInput, 10);
        if (!Number.isNaN(targetRow) && targetRow >= 1) {
          const zeroIdx = targetRow - 1;
          await this.loadRows(targetRow + 50);
          const effective = this.getEffectiveRowIndices();
          this.cursorRow = Math.min(zeroIdx, Math.max(0, effective.length - 1));
          this.scrollRow = Math.max(0, this.cursorRow - Math.floor(this.getVisibleDataRowCount() / 2));
        }
        this.mode = "normal";
        this.jumpInput = "";
      } else if (key === "\x1b" || key === "\x03") {
        this.mode = "normal";
        this.jumpInput = "";
      } else if (key === "\x7f" || key === "\b") {
        this.jumpInput = this.jumpInput.slice(0, -1);
      } else if (/^\d$/.test(key)) {
        this.jumpInput += key;
      }
      return;
    }

    // -------------------------------------------------------------
    // 3. Help Mode
    // -------------------------------------------------------------
    if (this.mode === "help") {
      if (key === "q" || key === "\x1b" || key === "?" || key === "\r" || key === " ") {
        this.mode = "normal";
      }
      return;
    }

    // -------------------------------------------------------------
    // 4. Normal Navigation Mode
    // -------------------------------------------------------------
    const effectiveIndices = this.getEffectiveRowIndices();
    const totalEffectiveRows = effectiveIndices.length;
    const pageRows = this.getVisibleDataRowCount();

    // Quit
    if (key === "q" || key === "Q" || key === "\x03") {
      this.exit();
      return;
    }

    // Help
    if (key === "?") {
      this.mode = "help";
      return;
    }

    // Enter Search
    if (key === "/") {
      this.mode = "search";
      this.searchInput = "";
      return;
    }

    // Jump to row
    if (key === ":" || key === "J") {
      this.mode = "jump";
      this.jumpInput = "";
      return;
    }

    // Clear search
    if (key === "\x1b") {
      if (this.searchQuery) {
        this.searchQuery = "";
        this.matchingRowIndices = null;
      }
      return;
    }

    // Next match (n) / Prev match (N)
    if (key === "n" && this.matchingRowIndices && this.matchingRowIndices.length > 0) {
      this.cursorRow = (this.cursorRow + 1) % this.matchingRowIndices.length;
      this.adjustScroll();
      return;
    }
    if (key === "N" && this.matchingRowIndices && this.matchingRowIndices.length > 0) {
      this.cursorRow = (this.cursorRow - 1 + this.matchingRowIndices.length) % this.matchingRowIndices.length;
      this.adjustScroll();
      return;
    }

    // Sort by selected column (s)
    if (key === "s" || key === "S") {
      const activeCol = this.columns[this.cursorCol];
      if (activeCol) {
        this.applySort(activeCol);
      }
      return;
    }

    // Toggle column auto-fit
    if (key === "c" || key === "C") {
      this.autoFitCols = !this.autoFitCols;
      return;
    }

    // Up / k / w
    if (key === "\x1b[A" || key === "k" || key === "w") {
      if (this.cursorRow > 0) {
        this.cursorRow--;
        this.adjustScroll();
      }
      return;
    }

    // Down / j
    if (key === "\x1b[B" || key === "j") {
      if (this.cursorRow < totalEffectiveRows - 1) {
        this.cursorRow++;
        this.adjustScroll();
      } else if (!this.isStreamExhausted) {
        await this.loadRows(this.loadedRows.length + 200);
        const updated = this.getEffectiveRowIndices();
        if (this.cursorRow < updated.length - 1) {
          this.cursorRow++;
          this.adjustScroll();
        }
      }
      return;
    }

    // Page Down / Space / f / Ctrl+F
    if (key === "\x1b[6~" || key === " " || key === "f" || key === "\x06") {
      const nextRow = Math.min(totalEffectiveRows - 1, this.cursorRow + pageRows);
      if (nextRow >= totalEffectiveRows - 5 && !this.isStreamExhausted) {
        await this.loadRows(this.loadedRows.length + pageRows * 2);
      }
      this.cursorRow = Math.min(this.getEffectiveRowIndices().length - 1, this.cursorRow + pageRows);
      this.adjustScroll();
      return;
    }

    // Page Up / b / Ctrl+B
    if (key === "\x1b[5~" || key === "b" || key === "\x02") {
      this.cursorRow = Math.max(0, this.cursorRow - pageRows);
      this.adjustScroll();
      return;
    }

    // Top / g / Home
    if (key === "\x1b[H" || key === "\x1b[1~" || key === "g") {
      this.cursorRow = 0;
      this.scrollRow = 0;
      return;
    }

    // End / G
    if (key === "\x1b[F" || key === "\x1b[4~" || key === "G") {
      // Load all or up to maxBufferRows
      while (!this.isStreamExhausted && this.loadedRows.length < (this.options.maxBufferRows || 25000)) {
        await this.loadRows(this.loadedRows.length + 1000);
      }
      const updated = this.getEffectiveRowIndices();
      this.cursorRow = Math.max(0, updated.length - 1);
      this.adjustScroll();
      return;
    }

    // Left / h / a
    if (key === "\x1b[D" || key === "h" || key === "a" || key === "\x1b[Z") {
      if (this.cursorCol > 0) {
        this.cursorCol--;
        if (this.cursorCol < this.scrollCol) {
          this.scrollCol = this.cursorCol;
        }
      }
      return;
    }

    // Right / l / d / Tab
    if (key === "\x1b[C" || key === "l" || key === "d" || key === "\t") {
      if (this.cursorCol < this.columns.length - 1) {
        this.cursorCol++;
        // Adjust scrollCol if needed in render
      }
      return;
    }
  }

  private adjustScroll(): void {
    const visibleDataRows = this.getVisibleDataRowCount();
    if (this.cursorRow < this.scrollRow) {
      this.scrollRow = this.cursorRow;
    } else if (this.cursorRow >= this.scrollRow + visibleDataRows) {
      this.scrollRow = this.cursorRow - visibleDataRows + 1;
    }
  }

  private getVisibleDataRowCount(): number {
    return Math.max(1, this.termRows - 4); // 1 title, 1 header, 1 divider, 1 status
  }

  private calculateColumnWidths(): Record<string, number> {
    const widths: Record<string, number> = {};
    const defaultWidth = 14;

    for (const col of this.columns) {
      widths[col] = Math.max(col.length, 8);
    }

    if (this.autoFitCols) {
      // Sample visible rows
      const indices = this.getEffectiveRowIndices();
      const sampleSlice = indices.slice(this.scrollRow, this.scrollRow + 50);
      for (const idx of sampleSlice) {
        const row = this.loadedRows[idx];
        if (!row) continue;
        for (const col of this.columns) {
          const val = row[col];
          const str = val === null || val === undefined ? "null" : String(val);
          widths[col] = Math.min(32, Math.max(widths[col] || 8, str.length));
        }
      }
    } else {
      for (const col of this.columns) {
        widths[col] = defaultWidth;
      }
    }

    return widths;
  }

  /**
   * Main rendering routine that constructs and prints a single terminal frame.
   */
  public render(): void {
    if (!this.isRunning) return;

    const visibleRowsCount = this.getVisibleDataRowCount();
    const effectiveIndices = this.getEffectiveRowIndices();
    const totalEffectiveRows = effectiveIndices.length;
    const colWidths = this.calculateColumnWidths();

    const rowNumWidth = Math.max(5, String(totalEffectiveRows).length + 2);
    let availableCols = this.termCols - rowNumWidth - 3; // for borders & padding

    // Determine visible columns based on scrollCol and available width
    const visibleCols: string[] = [];
    let usedColsWidth = 0;

    for (let c = this.scrollCol; c < this.columns.length; c++) {
      const colName = this.columns[c]!;
      const width = (colWidths[colName] || 12) + 2; // +2 for padding
      if (visibleCols.length > 0 && usedColsWidth + width > availableCols) {
        break;
      }
      visibleCols.push(colName);
      usedColsWidth += width;
    }

    // Adjust scrollCol if cursorCol is beyond visible columns
    if (this.cursorCol < this.scrollCol) {
      this.scrollCol = this.cursorCol;
    } else if (this.cursorCol >= this.scrollCol + visibleCols.length) {
      this.scrollCol = Math.max(0, this.cursorCol - visibleCols.length + 1);
    }

    const lines: string[] = [];

    // -------------------------------------------------------------
    // Line 1: Title & Summary Bar
    // -------------------------------------------------------------
    const titleText = this.options.title || this.options.filePath || "Rowpipe Interactive Viewer";
    const eofBadge = this.isStreamExhausted ? green("● EOF") : yellow("⏳ Streaming");
    const sortBadge = this.sortColumn
      ? magenta(` [Sort: ${this.sortColumn} ${this.sortAsc ? "▲" : "▼"}]`)
      : "";
    const searchBadge = this.searchQuery ? yellow(` [Filter: "${this.searchQuery}"]`) : "";

    const titleLeft = ` ${bold(cyan("ROWPIPE"))} ${dim("│")} ${bold(titleText)}${sortBadge}${searchBadge}`;
    const titleRight = `${eofBadge} ${dim("│")} ${formatNumber(totalEffectiveRows)} rows ${dim("│")} ${this.columns.length} cols `;
    const titlePadding = Math.max(0, this.termCols - stripAnsi(titleLeft).length - stripAnsi(titleRight).length);
    lines.push(bgGray(`${titleLeft}${" ".repeat(titlePadding)}${titleRight}`));

    // -------------------------------------------------------------
    // Line 2: Table Column Headers
    // -------------------------------------------------------------
    let headerLine = dim("#".padStart(rowNumWidth)) + " " + dim("│");
    for (const col of visibleCols) {
      const width = colWidths[col] || 12;
      const isActiveCol = this.columns[this.cursorCol] === col;
      const isSortedCol = this.sortColumn === col;
      const sortArrow = isSortedCol ? (this.sortAsc ? " ▲" : " ▼") : "";

      let colLabel = truncateVisible(col + sortArrow, width);
      colLabel = colLabel.padEnd(width);

      if (isActiveCol) {
        headerLine += ` ${bold(cyan(colLabel))} ${dim("│")}`;
      } else {
        headerLine += ` ${bold(colLabel)} ${dim("│")}`;
      }
    }
    lines.push(headerLine);

    // -------------------------------------------------------------
    // Line 3: Header Separator
    // -------------------------------------------------------------
    let divider = dim("─".repeat(rowNumWidth)) + dim("┼");
    for (const col of visibleCols) {
      const width = colWidths[col] || 12;
      divider += dim("─".repeat(width + 2)) + dim("┼");
    }
    lines.push(divider);

    // -------------------------------------------------------------
    // Lines 4+: Data Rows
    // -------------------------------------------------------------
    for (let i = 0; i < visibleRowsCount; i++) {
      const rowIdxInView = this.scrollRow + i;
      if (rowIdxInView >= totalEffectiveRows) {
        // Empty placeholder row
        lines.push(dim("~"));
        continue;
      }

      const originalRowIdx = effectiveIndices[rowIdxInView]!;
      const row = this.loadedRows[originalRowIdx];
      const isCursorRow = rowIdxInView === this.cursorRow;

      const rowNumStr = String(rowIdxInView + 1).padStart(rowNumWidth);
      let rowLine = isCursorRow ? bold(cyan(rowNumStr)) : dim(rowNumStr);
      rowLine += " " + dim("│");

      for (const col of visibleCols) {
        const width = colWidths[col] || 12;
        const val = row?.[col];
        let cellText = "";

        if (val === null || val === undefined) {
          cellText = dim("null").padEnd(width);
        } else if (typeof val === "number" || typeof val === "bigint") {
          cellText = yellow(String(val)).padStart(width);
        } else if (typeof val === "boolean") {
          cellText = magenta(val ? "true" : "false").padEnd(width);
        } else {
          const str = String(val);
          const truncated = truncateVisible(str, width).padEnd(width);
          // Highlight search matches
          if (this.searchQuery && str.toLowerCase().includes(this.searchQuery.toLowerCase())) {
            cellText = bold(yellow(truncated));
          } else {
            cellText = truncated;
          }
        }

        rowLine += ` ${cellText} ${dim("│")}`;
      }

      if (isCursorRow) {
        lines.push(inverse(rowLine));
      } else {
        lines.push(rowLine);
      }
    }

    // -------------------------------------------------------------
    // Bottom Status / Command Bar
    // -------------------------------------------------------------
    let statusBar = "";
    if (this.mode === "search") {
      statusBar = bgBlue(` SEARCH: /${this.searchInput}█ `) + ` (Enter: apply, Esc: cancel, n/N: next/prev)`;
    } else if (this.mode === "jump") {
      statusBar = bgBlue(` JUMP TO ROW #: ${this.jumpInput}█ `) + ` (Enter: go, Esc: cancel)`;
    } else if (this.mode === "help") {
      statusBar = bgBlue(" HELP MODE ") + ` (Press ? or Esc to close)`;
    } else {
      const posInfo = `Row ${formatNumber(this.cursorRow + 1)}/${formatNumber(totalEffectiveRows)} │ Col ${this.cursorCol + 1}/${this.columns.length}`;
      const shortcuts = `[?]Help [/]Search [s]Sort [c]Width [g/G]Top/End [:]Jump [q]Quit`;
      const padding = Math.max(1, this.termCols - posInfo.length - shortcuts.length - 3);
      statusBar = bgGray(` ${posInfo}${" ".repeat(padding)}${shortcuts} `);
    }

    lines.push(statusBar);

    // -------------------------------------------------------------
    // Modal Overlay for Help Mode
    // -------------------------------------------------------------
    if (this.mode === "help") {
      this.drawHelpModal(lines);
    }

    // Frame output to terminal
    process.stdout.write(ANSI.cursorHome + lines.slice(0, this.termRows).join("\n"));
  }

  private drawHelpModal(lines: string[]): void {
    const helpBox = [
      "┌────────────────────────────────────────────────────────┐",
      "│              ROWPIPE INTERACTIVE VIEWER HELP           │",
      "├────────────────────────────────────────────────────────┤",
      "│  Navigation:                                           │",
      "│    ↑ / k / w        : Move cursor up 1 row             │",
      "│    ↓ / j / s        : Move cursor down 1 row           │",
      "│    ← / h / a        : Scroll left column               │",
      "│    → / l / d / Tab  : Scroll right column              │",
      "│    PageUp / b       : Page up                          │",
      "│    PageDown / Space : Page down                        │",
      "│    Home / g         : Jump to top of dataset           │",
      "│    End / G          : Jump to end of loaded stream     │",
      "│                                                        │",
      "│  Data Operations:                                      │",
      "│    /                : Live text search & row filter    │",
      "│    n / N            : Jump to next / previous match    │",
      "│    s                : Cycle sort order on active col   │",
      "│    c                : Toggle auto-fit column width     │",
      "│    : or J           : Jump to specific row number      │",
      "│                                                        │",
      "│  Exit:                                                 │",
      "│    q / Esc / Ctrl+C : Quit viewer & restore terminal   │",
      "└────────────────────────────────────────────────────────┘",
    ];

    const startRow = Math.max(2, Math.floor((this.termRows - helpBox.length) / 2));
    const startCol = Math.max(0, Math.floor((this.termCols - 60) / 2));

    for (let r = 0; r < helpBox.length; r++) {
      const lineIdx = startRow + r;
      if (lineIdx < lines.length) {
        const boxLine = helpBox[r]!;
        const currentLine = lines[lineIdx] || "";
        const paddedBox = " ".repeat(startCol) + bold(cyan(boxLine));
        lines[lineIdx] = paddedBox;
      }
    }
  }

  /**
   * Non-interactive fallback printer for scripts, CI, and non-TTY pipes.
   */
  public renderNonInteractive(maxRows = 50): void {
    const colWidths = this.calculateColumnWidths();
    const rowsToPrint = this.loadedRows.slice(0, maxRows);

    let headerLine = dim("#".padStart(5)) + " │";
    for (const col of this.columns) {
      const width = colWidths[col] || 12;
      headerLine += ` ${bold(col.padEnd(width))} │`;
    }

    let divider = dim("─".repeat(5)) + "┼";
    for (const col of this.columns) {
      const width = colWidths[col] || 12;
      divider += "─".repeat(width + 2) + "┼";
    }

    process.stdout.write(headerLine + "\n");
    process.stdout.write(divider + "\n");

    for (let i = 0; i < rowsToPrint.length; i++) {
      const row = rowsToPrint[i]!;
      let rowLine = dim(String(i + 1).padStart(5)) + " │";
      for (const col of this.columns) {
        const width = colWidths[col] || 12;
        const val = row[col];
        const cell = val === null || val === undefined ? "null" : String(val);
        rowLine += ` ${cell.padEnd(width)} │`;
      }
      process.stdout.write(rowLine + "\n");
    }

    if (this.loadedRows.length > maxRows || !this.isStreamExhausted) {
      process.stdout.write(
        dim(`\nShowing first ${rowsToPrint.length} rows (Stream has more rows). Run in interactive terminal to explore full dataset.\n`)
      );
    }
  }
}
