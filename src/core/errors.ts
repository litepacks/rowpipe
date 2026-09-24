export class RowpipeError extends Error {
  public readonly exitCode: number;

  constructor(message: string, exitCode = 1) {
    super(message);
    this.name = "RowpipeError";
    this.exitCode = exitCode;
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export class InvalidArgumentError extends RowpipeError {
  constructor(message: string) {
    super(message, 2);
    this.name = "InvalidArgumentError";
  }
}

export interface ParseErrorContext {
  file?: string;
  sheet?: string;
  row?: number;
  column?: string | number;
  byteOffset?: number;
  line?: number;
  cell?: string;
}

export class ParseError extends RowpipeError {
  public readonly context: ParseErrorContext;

  constructor(message: string, context: ParseErrorContext = {}) {
    let detailedMsg = `Parse error: ${message}`;
    const parts: string[] = [];

    if (context.file) parts.push(`File: ${context.file}`);
    if (context.sheet) parts.push(`Sheet: ${context.sheet}`);
    if (context.row !== undefined) parts.push(`Row: ${context.row}`);
    if (context.column !== undefined) parts.push(`Column: ${context.column}`);
    if (context.cell) parts.push(`Cell: ${context.cell}`);
    if (context.byteOffset !== undefined) parts.push(`Byte offset: ${context.byteOffset}`);

    if (parts.length > 0) {
      detailedMsg += `\n  Context: ${parts.join(", ")}`;
    }

    super(detailedMsg, 3);
    this.name = "ParseError";
    this.context = context;
  }
}

export interface ValidationViolation {
  column: string;
  expected: string;
  actual?: string;
  count: number;
  examples?: unknown[];
}

export class ValidationError extends RowpipeError {
  public readonly violations: ValidationViolation[];
  public readonly totalRows: number;
  public readonly validRows: number;
  public readonly invalidRows: number;

  constructor(
    message: string,
    violations: ValidationViolation[] = [],
    totalRows = 0,
    validRows = 0,
    invalidRows = 0
  ) {
    super(message, 4);
    this.name = "ValidationError";
    this.violations = violations;
    this.totalRows = totalRows;
    this.validRows = validRows;
    this.invalidRows = invalidRows;
  }
}

export interface DuplicateKeyContext {
  dataset: "left" | "right";
  key: string;
  rows: number[];
  filePath?: string;
}

export class DuplicateKeyError extends RowpipeError {
  public readonly context: DuplicateKeyContext;

  constructor(context: DuplicateKeyContext) {
    const msg = `Duplicate key detected\n  Dataset: ${context.dataset}${context.filePath ? ` (${context.filePath})` : ""}\n  Key: ${context.key}\n  Rows: ${context.rows.join(", ")}`;
    super(msg, 5);
    this.name = "DuplicateKeyError";
    this.context = context;
  }
}

export class DiffMismatchError extends RowpipeError {
  public readonly differencesFound: number;

  constructor(differencesFound: number) {
    super(`Datasets have ${differencesFound} difference(s).`, 1);
    this.name = "DiffMismatchError";
    this.differencesFound = differencesFound;
  }
}

export interface FileSystemErrorContext {
  path?: string;
  operation?: string;
  cause?: string | Error;
}

export class FileSystemError extends RowpipeError {
  public readonly context: FileSystemErrorContext;

  constructor(message: string, context: FileSystemErrorContext = {}) {
    let detailedMsg = `Filesystem error: ${message}`;
    const parts: string[] = [];

    if (context.path) parts.push(`Path: ${context.path}`);
    if (context.operation) parts.push(`Operation: ${context.operation}`);
    if (context.cause) {
      const causeStr = context.cause instanceof Error ? context.cause.message : String(context.cause);
      parts.push(`Cause: ${causeStr}`);
    }

    if (parts.length > 0) {
      detailedMsg += `\n  ${parts.join("\n  ")}`;
    }

    super(detailedMsg, 1);
    this.name = "FileSystemError";
    this.context = context;
  }
}

