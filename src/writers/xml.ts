import { Writable } from "node:stream";
import { openCompressedWriteStream, closeWritableStream } from "../utils/compression.js";
import type { DataBatch, DataStream, Row, TabularWriter, WriterOptions } from "../core/types.js";

export interface XMLWriterOptions extends WriterOptions {
  xmlRoot?: string;
  xmlRow?: string;
  declaration?: boolean;
  indent?: string;
}

export function escapeXml(unsafe: unknown): string {
  if (unsafe === null || unsafe === undefined) return "";
  const str = String(unsafe);
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

export function sanitizeXmlTagName(name: string): string {
  let clean = name.replace(/[^a-zA-Z0-9_\-\.]/g, "_");
  if (/^[^a-zA-Z_]/.test(clean)) {
    clean = "_" + clean;
  }
  return clean || "field";
}

function serializeValueToXml(key: string, val: unknown, indent: string, level: number): string {
  const tag = sanitizeXmlTagName(key);
  const pad = indent.repeat(level);

  if (val === null || val === undefined) {
    return `${pad}<${tag}/>\n`;
  }

  if (Array.isArray(val)) {
    let out = "";
    for (const item of val) {
      out += serializeValueToXml(tag, item, indent, level);
    }
    return out;
  }

  if (val instanceof Date) {
    return `${pad}<${tag}>${escapeXml(val.toISOString())}</${tag}>\n`;
  }

  if (typeof val === "object" && !Buffer.isBuffer(val)) {
    let out = `${pad}<${tag}>\n`;
    for (const [k, v] of Object.entries(val as Record<string, unknown>)) {
      out += serializeValueToXml(k, v, indent, level + 1);
    }
    out += `${pad}</${tag}>\n`;
    return out;
  }

  if (Buffer.isBuffer(val) || val instanceof Uint8Array) {
    return `${pad}<${tag}>${Buffer.from(val).toString("base64")}</${tag}>\n`;
  }

  return `${pad}<${tag}>${escapeXml(val)}</${tag}>\n`;
}

function serializeRowToXml(row: Row, rowTag: string, indent: string): string {
  const sanitizedRowTag = sanitizeXmlTagName(rowTag);
  const attrParts: string[] = [];
  const childParts: string[] = [];

  for (const [key, val] of Object.entries(row)) {
    if (key.startsWith("@") && typeof val !== "object") {
      const attrName = sanitizeXmlTagName(key.slice(1));
      attrParts.push(`${attrName}="${escapeXml(val)}"`);
    } else {
      childParts.push(serializeValueToXml(key, val, indent, 2));
    }
  }

  const openTag = attrParts.length > 0 ? `${sanitizedRowTag} ${attrParts.join(" ")}` : sanitizedRowTag;

  if (childParts.length === 0) {
    return `${indent}<${openTag}/>\n`;
  }

  return `${indent}<${openTag}>\n${childParts.join("")}${indent}</${sanitizedRowTag}>\n`;
}

/**
 * Incremental, stream-first XML Writer.
 * Writes records row-by-row directly into the output stream without buffering the dataset in memory.
 */
export class XMLWriter implements TabularWriter {
  private output: Writable | string;
  private options: XMLWriterOptions;
  private outStream?: Writable;

  constructor(output: Writable | string | any, options: XMLWriterOptions = {}) {
    if (
      typeof output === "object" &&
      output !== null &&
      !(output instanceof Writable) &&
      ("outputPath" in output || "output" in output)
    ) {
      this.output = output.outputPath || output.output || "-";
      this.options = { ...output, ...options };
    } else {
      this.output = output;
      this.options = { ...options };
    }
  }

  private async writeChunk(chunk: string): Promise<void> {
    if (!this.outStream) return;
    if (!this.outStream.write(chunk)) {
      await new Promise<void>((resolve) => this.outStream!.once("drain", resolve));
    }
  }

  async write(dataStream: DataStream | any, options?: WriterOptions): Promise<void> {
    const mergedOptions: XMLWriterOptions = {
      ...this.options,
      ...options,
    };

    let stream: DataStream;
    if (Array.isArray(dataStream)) {
      const arr = dataStream;
      stream = (async function* () {
        yield { rows: arr, offset: 0 };
      })();
    } else if (
      dataStream &&
      (Symbol.asyncIterator in Object(dataStream) || Symbol.iterator in Object(dataStream))
    ) {
      stream = (async function* () {
        for await (const item of (dataStream as any)) {
          if (item && Array.isArray((item as any).rows)) {
            yield item as DataBatch;
          } else if (Array.isArray(item)) {
            yield { rows: item, offset: 0 };
          } else if (item) {
            yield { rows: [item], offset: 0 };
          }
        }
      })();
    } else {
      stream = dataStream;
    }

    this.outStream = openCompressedWriteStream(this.output, mergedOptions);

    const rootTag = sanitizeXmlTagName(mergedOptions.xmlRoot || "rows");
    const rowTag = sanitizeXmlTagName(mergedOptions.xmlRow || "row");
    const indent = mergedOptions.indent ?? "  ";

    if (mergedOptions.declaration !== false) {
      await this.writeChunk('<?xml version="1.0" encoding="UTF-8"?>\n');
    }
    await this.writeChunk(`<${rootTag}>\n`);

    for await (const batch of stream) {
      if (batch.rows.length === 0) continue;

      let batchBuffer = "";
      for (const row of batch.rows) {
        batchBuffer += serializeRowToXml(row, rowTag, indent);
        if (batchBuffer.length >= 65536) {
          await this.writeChunk(batchBuffer);
          batchBuffer = "";
        }
      }

      if (batchBuffer.length > 0) {
        await this.writeChunk(batchBuffer);
      }
    }

    await this.writeChunk(`</${rootTag}>\n`);
  }

  async close(): Promise<void> {
    if (this.outStream && this.outStream !== process.stdout) {
      await closeWritableStream(this.outStream);
    }
  }
}
