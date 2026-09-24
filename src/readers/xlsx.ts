import type { Readable } from "node:stream";
import readXlsxFile from "read-excel-file/node";
import { InvalidArgumentError, ParseError } from "../core/errors.js";
import type {
  DataBatch,
  DataStream,
  InspectionMetadata,
  ReaderOptions,
  Row,
  SheetMetadata,
  TabularReader,
} from "../core/types.js";

export interface XLSXReaderOptions extends ReaderOptions {
  sheet?: string | number;
  filePath?: string;
}

export class XLSXReader implements TabularReader {
  private input: Readable | string;
  private options: XLSXReaderOptions;

  constructor(input: Readable | string, options: XLSXReaderOptions = {}) {
    this.input = input;
    this.options = { ...options };
    if (typeof input === "string") {
      this.options.filePath = input;
    }
  }

  async getParsedSheets(): Promise<Array<{ sheet: string; data: unknown[][] }>> {
    try {
      const result = await (readXlsxFile as unknown as (input: unknown, options?: unknown) => Promise<unknown>)(
        this.input,
        { getSheets: true }
      );
      if (Array.isArray(result)) {
        if (result.length > 0 && typeof result[0] === "object" && result[0] !== null && "sheet" in result[0]) {
          return result as Array<{ sheet: string; data: unknown[][] }>;
        }
        return [{ sheet: "Sheet1", data: result as unknown[][] }];
      }
      return [];
    } catch (err) {
      throw new ParseError(`Failed to parse XLSX: ${(err as Error).message}`, {
        file: this.options.filePath,
      });
    }
  }

  async *read(options?: ReaderOptions): DataStream {
    const mergedOptions: XLSXReaderOptions = {
      ...this.options,
      ...options,
    };
    const batchSize = Math.max(1, mergedOptions.batchSize ?? 1000);
    const targetSheet = mergedOptions.sheet;

    const sheets = await this.getParsedSheets();
    if (sheets.length === 0) {
      return;
    }

    let selectedSheet: { sheet: string; data: unknown[][] } | undefined;

    if (targetSheet === undefined || targetSheet === null) {
      selectedSheet = sheets[0];
    } else if (typeof targetSheet === "number") {
      selectedSheet = sheets[targetSheet - 1];
    } else {
      selectedSheet = sheets.find(
        (s) => s.sheet.toLowerCase() === String(targetSheet).toLowerCase()
      );
    }

    if (!selectedSheet) {
      throw new InvalidArgumentError(
        `Sheet "${targetSheet}" was not found in the workbook. Available sheets: ${sheets.map((s) => s.sheet).join(", ")}`
      );
    }

    const grid = selectedSheet.data;
    if (grid.length === 0) return;

    let headers: string[] | null = null;
    let rowsInCurrentBatch: Row[] = [];
    let globalOffset = 0;

    for (let r = 0; r < grid.length; r++) {
      const rowCells = grid[r] || [];
      if (rowCells.length === 0 || rowCells.every((c) => c === null || c === undefined || c === "")) {
        continue;
      }

      if (headers === null) {
        headers = rowCells.map((c, i) => {
          const str = c !== null && c !== undefined ? String(c).trim() : "";
          return str.length > 0 ? str : `col_${i + 1}`;
        });
        continue;
      }

      const rowObj: Row = {};
      for (let i = 0; i < headers.length; i++) {
        const key = headers[i] || `col_${i + 1}`;
        const cellVal = rowCells[i];
        rowObj[key] = cellVal !== null && cellVal !== undefined ? cellVal : "";
      }

      rowsInCurrentBatch.push(rowObj);

      if (rowsInCurrentBatch.length >= batchSize) {
        yield {
          rows: rowsInCurrentBatch,
          offset: globalOffset,
        };
        globalOffset += rowsInCurrentBatch.length;
        rowsInCurrentBatch = [];
      }
    }

    if (rowsInCurrentBatch.length > 0) {
      yield {
        rows: rowsInCurrentBatch,
        offset: globalOffset,
      };
    }
  }

  async inspect(options?: ReaderOptions): Promise<InspectionMetadata> {
    const sheets = await this.getParsedSheets();
    const targetSheet = options?.sheet ?? this.options.sheet;

    const sheetSummaries: SheetMetadata[] = sheets.map((s) => {
      const nonHeaderRows = Math.max(0, s.data.length - 1);
      const headerRow = s.data[0] || [];
      const colHeaders = headerRow.map((c, i) =>
        c !== null && c !== undefined ? String(c).trim() : `col_${i + 1}`
      );

      return {
        name: s.sheet,
        rowCount: nonHeaderRows,
        columnCount: colHeaders.length,
        columns: colHeaders,
      };
    });

    let selectedSheet = sheets[0];
    if (targetSheet !== undefined && targetSheet !== null) {
      selectedSheet =
        typeof targetSheet === "number"
          ? sheets[targetSheet - 1]
          : sheets.find((s) => s.sheet.toLowerCase() === String(targetSheet).toLowerCase());
    }

    let columns: Array<{ name: string; type: any; nullPercentage: number }> = [];
    let rowCount = 0;

    if (selectedSheet && selectedSheet.data.length > 0) {
      const headerRow = selectedSheet.data[0] || [];
      const colHeaders = headerRow.map((c, i) =>
        c !== null && c !== undefined ? String(c).trim() : `col_${i + 1}`
      );
      rowCount = Math.max(0, selectedSheet.data.length - 1);
      columns = colHeaders.map((name) => ({
        name,
        type: "string",
        nullPercentage: 0,
      }));
    }

    return {
      format: "XLSX",
      rowCount,
      columnCount: columns.length,
      columns,
      sheetsCount: sheets.length,
      sheets: sheetSummaries,
    };
  }
}
