import { openSync, readSync, closeSync } from "node:fs";

/**
 * Sniffs tabular data format from an in-memory buffer (magic bytes / header inspection).
 */
export function sniffFormatFromBuffer(buffer: Buffer | Uint8Array): string | null {
  if (!buffer || buffer.length === 0) return null;
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer);

  // 1. Parquet: "PAR1" (4 bytes)
  if (buf.length >= 4 && buf.subarray(0, 4).toString("ascii") === "PAR1") {
    return "parquet";
  }

  // 2. Arrow IPC / Feather: "ARROW1" (6 bytes)
  if (buf.length >= 6 && buf.subarray(0, 6).toString("ascii") === "ARROW1") {
    return "arrow";
  }

  // 3. Avro Object Container: "Obj\x01" (4 bytes: 0x4F, 0x62, 0x6A, 0x01)
  if (
    buf.length >= 4 &&
    buf[0] === 0x4F &&
    buf[1] === 0x62 &&
    buf[2] === 0x6A &&
    buf[3] === 0x01
  ) {
    return "avro";
  }

  // 4. SQLite database file: "SQLite format 3\0" (16 bytes)
  if (
    buf.length >= 16 &&
    buf.subarray(0, 15).toString("ascii") === "SQLite format 3"
  ) {
    return "sqlite";
  }

  // 5. XLSX (ZIP container): "PK\x03\x04"
  if (
    buf.length >= 4 &&
    buf[0] === 0x50 &&
    buf[1] === 0x4B &&
    buf[2] === 0x03 &&
    buf[3] === 0x04
  ) {
    return "xlsx";
  }

  // Text-based heuristics
  let text = buf.toString("utf8");
  if (text.charCodeAt(0) === 0xFEFF) {
    text = text.slice(1);
  }
  const trimmed = text.trimStart();

  // 6. XML: starts with <?xml or <tag>
  if (trimmed.startsWith("<?xml") || (trimmed.startsWith("<") && !trimmed.startsWith("<!"))) {
    return "xml";
  }

  // 7. JSON array
  if (trimmed.startsWith("[")) {
    return "json";
  }

  // 8. JSON object or NDJSON / JSONL
  if (trimmed.startsWith("{")) {
    const newlineIdx = text.indexOf("\n");
    if (newlineIdx !== -1) {
      const firstLine = text.slice(0, newlineIdx).trim();
      if (firstLine.startsWith("{") && firstLine.endsWith("}")) {
        return "ndjson";
      }
    }
    return "json";
  }

  // 9. TSV vs PSV
  if (trimmed.length > 0) {
    const firstLine = trimmed.split("\n")[0] || "";
    if (firstLine.includes("\t") && !firstLine.includes(",")) {
      return "tsv";
    }
    if (firstLine.includes("|") && !firstLine.includes(",")) {
      return "psv";
    }
  }

  return null;
}

/**
 * Sniffs format of a local file by reading its initial 64 bytes.
 * Returns null if file does not exist, cannot be read, or format cannot be identified.
 */
export function sniffFormatFromFile(filePath: string): string | null {
  try {
    const fd = openSync(filePath, "r");
    try {
      const buffer = Buffer.alloc(64);
      const bytesRead = readSync(fd, buffer, 0, 64, 0);
      if (bytesRead > 0) {
        return sniffFormatFromBuffer(buffer.subarray(0, bytesRead));
      }
    } finally {
      closeSync(fd);
    }
  } catch {
    // Graceful fallback for non-existent or unreadable paths
  }
  return null;
}
