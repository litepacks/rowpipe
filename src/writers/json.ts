import { closeWritableStream, openCompressedWriteStream } from "../utils/compression.js";
import type { Writable } from "node:stream";
import type { DataStream, TabularWriter, WriterOptions } from "../core/types.js";

import { safeJsonStringify } from "../utils/formatting.js";

export class JSONWriter implements TabularWriter {
  private output: Writable | string;
  private createdStream?: Writable;

  constructor(output: Writable | string) {
    this.output = output;
  }

  private getOutputStream(opts?: WriterOptions): Writable {
    const stream = openCompressedWriteStream(this.output, opts);
    if (typeof this.output === "string") {
      this.createdStream = stream;
    }
    return stream;
  }

  private async writeChunk(stream: Writable, data: string): Promise<void> {
    if (!stream.write(data)) {
      await new Promise<void>((resolve) => stream.once("drain", resolve));
    }
  }

  async write(dataStream: DataStream, options?: WriterOptions): Promise<void> {
    const outStream = this.getOutputStream(options);
    let isFirstRow = true;

    await this.writeChunk(outStream, "[\n");

    for await (const batch of dataStream) {
      for (const row of batch.rows) {
        const prefix = isFirstRow ? "  " : ",\n  ";
        isFirstRow = false;
        await this.writeChunk(outStream, `${prefix}${safeJsonStringify(row)}`);
      }
    }

    await this.writeChunk(outStream, "\n]\n");
  }

  async close(): Promise<void> {
    const s = this.createdStream || (typeof this.output !== "string" ? this.output : undefined);
    if (s && s !== process.stdout) {
      await closeWritableStream(s);
    }
  }
}
