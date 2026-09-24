import { mkdirSync, existsSync } from "node:fs";
import { dirname } from "node:path";
import type { DataStream, Row, TabularWriter } from "../core/types.js";
import { createWriter } from "../writers/index.js";

export interface SplitOptions {
  chunkSize?: number;
  outPattern: string;
  format?: string;
  delimiter?: string;
  sheet?: string;
}

function formatChunkPath(pattern: string, partIndex: number): string {
  let result = pattern;

  // Handle zero-padded formatting like {n:03d}, {n:04d}
  result = result.replace(/\{n(?::0?(\d+)d)?\}/g, (_match, padStr) => {
    const padLen = padStr ? parseInt(padStr, 10) : 0;
    return padLen > 0 ? String(partIndex).padStart(padLen, "0") : String(partIndex);
  });

  // If no placeholder was present, append _partN before extension
  if (!pattern.includes("{n")) {
    const lastDot = pattern.lastIndexOf(".");
    if (lastDot > 0) {
      result = `${pattern.slice(0, lastDot)}_part${partIndex}${pattern.slice(lastDot)}`;
    } else {
      result = `${pattern}_part${partIndex}`;
    }
  }

  return result;
}

/**
 * Splits an input stream into sequential chunk files based on row count.
 */
export async function splitStream(
  stream: DataStream,
  options: SplitOptions
): Promise<{ fileCount: number; rowCount: number; files: string[] }> {
  const chunkSize = Math.max(1, options.chunkSize || 100000);
  const createdFiles: string[] = [];

  let partIndex = 1;
  let currentChunkRows: Row[] = [];
  let currentWriter: TabularWriter | null = null;
  let totalRows = 0;

  async function openWriterForPart(index: number): Promise<TabularWriter> {
    const filePath = formatChunkPath(options.outPattern, index);
    const dir = dirname(filePath);
    if (dir && dir !== "." && !existsSync(dir)) {
      mkdirSync(dir, { recursive: true });
    }
    createdFiles.push(filePath);
    return createWriter(filePath, {
      format: options.format,
      delimiter: options.delimiter,
    });
  }

  for await (const batch of stream) {
    for (const row of batch.rows) {
      if (!currentWriter) {
        currentWriter = await openWriterForPart(partIndex);
      }

      currentChunkRows.push(row);
      totalRows++;

      if (currentChunkRows.length >= chunkSize) {
        const rowsToWrite = currentChunkRows;
        currentChunkRows = [];

        async function* chunkBatch() {
          yield { rows: rowsToWrite, offset: 0 };
        }
        await currentWriter.write(chunkBatch());
        if (currentWriter.close) {
          await currentWriter.close();
        }
        currentWriter = null;
        partIndex++;
      }
    }
  }

  // Flush any remaining rows in the last chunk
  if (currentChunkRows.length > 0) {
    if (!currentWriter) {
      currentWriter = await openWriterForPart(partIndex);
    }
    const rowsToWrite = currentChunkRows;
    currentChunkRows = [];

    async function* chunkBatch() {
      yield { rows: rowsToWrite, offset: 0 };
    }
    await currentWriter.write(chunkBatch());
  }

  if (currentWriter && currentWriter.close) {
    await currentWriter.close();
  }

  return {
    fileCount: createdFiles.length,
    rowCount: totalRows,
    files: createdFiles,
  };
}
