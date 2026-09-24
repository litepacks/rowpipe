import { createReadStream, createWriteStream } from "node:fs";
import { Readable, Writable, Transform, PassThrough, type TransformCallback } from "node:stream";
import {
  createGzip,
  createGunzip,
  createBrotliCompress,
  createBrotliDecompress,
  createDeflate,
  createInflate,
} from "node:zlib";
import { init as initZstdWasm, compress as compressZstd } from "@bokuweb/zstd-wasm";
import { Decompress as ZstdDecompressEngine } from "fzstd";
import { InvalidArgumentError } from "../core/errors.js";

export type CompressionAlgorithm = "gzip" | "brotli" | "zstd" | "deflate" | "none";

export interface CompressionOptions {
  gzip?: boolean;
  brotli?: boolean;
  zstd?: boolean;
  deflate?: boolean;
  compression?: CompressionAlgorithm | string;
  compress?: CompressionAlgorithm | string;
  filePath?: string;
  level?: number;
}

let zstdWasmInitPromise: Promise<void> | null = null;
export async function ensureZstdInitialized(): Promise<void> {
  if (!zstdWasmInitPromise) {
    zstdWasmInitPromise = initZstdWasm();
  }
  await zstdWasmInitPromise;
}

/**
 * Streaming Zstandard decompression transform using pure-JS fzstd engine.
 * Delivers bounded O(1) memory and non-blocking streaming decompression.
 */
export class ZstdDecompressStream extends Transform {
  private decompressor: InstanceType<typeof ZstdDecompressEngine>;

  constructor() {
    super();
    this.decompressor = new ZstdDecompressEngine((chunk: Uint8Array) => {
      this.push(Buffer.from(chunk));
    });
  }

  _transform(chunk: Buffer | Uint8Array, _encoding: BufferEncoding, callback: TransformCallback): void {
    try {
      const u8 = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
      this.decompressor.push(u8, false);
      callback();
    } catch (err: any) {
      callback(err);
    }
  }

  _flush(callback: TransformCallback): void {
    try {
      this.decompressor.push(new Uint8Array(0), true);
      callback();
    } catch (err: any) {
      callback(err);
    }
  }
}

/**
 * Streaming Zstandard compression transform using high-performance zstd-wasm.
 * Buffers chunks up to 64KB blocks and flushes on stream completion.
 */
export class ZstdCompressStream extends Transform {
  private level: number;
  private chunks: Buffer[] = [];
  private totalLength = 0;
  private maxBlockSize: number;

  constructor(options: { level?: number; blockSize?: number } = {}) {
    super();
    this.level = options.level ?? 3;
    this.maxBlockSize = options.blockSize ?? 65536;
  }

  async _transform(chunk: Buffer | Uint8Array, _encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
    try {
      await ensureZstdInitialized();
      const buf = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      this.chunks.push(buf);
      this.totalLength += buf.length;

      if (this.totalLength >= this.maxBlockSize) {
        const merged = Buffer.concat(this.chunks);
        this.chunks = [];
        this.totalLength = 0;
        const compressed = compressZstd(merged, this.level);
        this.push(Buffer.from(compressed));
      }
      callback();
    } catch (err: any) {
      callback(err);
    }
  }

  async _flush(callback: TransformCallback): Promise<void> {
    try {
      await ensureZstdInitialized();
      if (this.chunks.length > 0) {
        const merged = Buffer.concat(this.chunks);
        this.chunks = [];
        this.totalLength = 0;
        const compressed = compressZstd(merged, this.level);
        this.push(Buffer.from(compressed));
      }
      callback();
    } catch (err: any) {
      callback(err);
    }
  }
}

/**
 * Infers compression algorithm from file path extension.
 */
export function inferCompressionFromPath(filePath: string): CompressionAlgorithm | null {
  const lower = filePath.toLowerCase();
  if (lower.endsWith(".gz") || lower.endsWith(".gzip")) return "gzip";
  if (lower.endsWith(".br") || lower.endsWith(".brotli")) return "brotli";
  if (lower.endsWith(".zst") || lower.endsWith(".zstd")) return "zstd";
  if (lower.endsWith(".zz") || lower.endsWith(".deflate")) return "deflate";
  return null;
}

export function isGzipPath(filePath: string): boolean {
  return inferCompressionFromPath(filePath) === "gzip";
}

export function isBrotliPath(filePath: string): boolean {
  return inferCompressionFromPath(filePath) === "brotli";
}

export function isZstdPath(filePath: string): boolean {
  return inferCompressionFromPath(filePath) === "zstd";
}

export function isCompressedPath(filePath: string): boolean {
  return inferCompressionFromPath(filePath) !== null;
}

/**
 * Strips compression extensions (.gz, .gzip, .br, .brotli, .zst, .zstd, .zz, .deflate)
 * to inspect the underlying tabular format extension.
 */
export function stripCompressionExtension(filePath: string): string {
  return filePath.replace(/\.(gz|gzip|br|brotli|zst|zstd|zz|deflate)$/i, "");
}

/**
 * Resolves effective compression algorithm from path and options.
 */
export function resolveCompression(
  filePath?: string,
  options?: CompressionOptions
): CompressionAlgorithm {
  const explicit = (options?.compression || options?.compress)?.toLowerCase();
  if (explicit) {
    if (explicit === "gz" || explicit === "gzip") return "gzip";
    if (explicit === "br" || explicit === "brotli") return "brotli";
    if (explicit === "zst" || explicit === "zstd" || explicit === "zstandard") return "zstd";
    if (explicit === "zz" || explicit === "deflate") return "deflate";
    if (explicit === "none" || explicit === "uncompressed") return "none";
    throw new InvalidArgumentError(`Unsupported compression algorithm: "${explicit}"`);
  }

  if (options?.gzip === true) return "gzip";
  if (options?.brotli === true) return "brotli";
  if (options?.zstd === true) return "zstd";
  if (options?.deflate === true) return "deflate";

  if (filePath && typeof filePath === "string" && filePath !== "-") {
    const inferred = inferCompressionFromPath(filePath);
    if (inferred) return inferred;
  }

  return "none";
}

/**
 * Creates a decompressor stream transform for given algorithm.
 */
export function createDecompressStream(algorithm: CompressionAlgorithm): Transform {
  switch (algorithm) {
    case "gzip":
      return createGunzip();
    case "brotli":
      return createBrotliDecompress();
    case "zstd":
      return new ZstdDecompressStream();
    case "deflate":
      return createInflate();
    case "none":
      return new PassThrough();
    default:
      throw new InvalidArgumentError(`Unsupported decompression algorithm: "${algorithm}"`);
  }
}

/**
 * Creates a compressor stream transform for given algorithm.
 */
export function createCompressStream(algorithm: CompressionAlgorithm, options?: { level?: number }): Transform {
  switch (algorithm) {
    case "gzip":
      return createGzip(options?.level !== undefined ? { level: options.level } : undefined);
    case "brotli":
      return createBrotliCompress();
    case "zstd":
      return new ZstdCompressStream({ level: options?.level });
    case "deflate":
      return createDeflate(options?.level !== undefined ? { level: options.level } : undefined);
    case "none":
      return new PassThrough();
    default:
      throw new InvalidArgumentError(`Unsupported compression algorithm: "${algorithm}"`);
  }
}

/**
 * Opens a readable stream for a file or wraps an existing stream with transparent decompression.
 */
export function openDecompressedReadStream(
  input: Readable | string,
  options: CompressionOptions = {}
): Readable {
  if (typeof input === "string") {
    const algo = resolveCompression(input, options);
    if (input === "-") {
      if (algo !== "none") {
        const decomp = createDecompressStream(algo);
        return process.stdin.pipe(decomp);
      }
      return process.stdin;
    }
    const rawStream = createReadStream(input);
    if (algo !== "none") {
      const decomp = createDecompressStream(algo);
      return rawStream.pipe(decomp);
    }
    return rawStream;
  }

  const algo = resolveCompression(undefined, options);
  if (algo !== "none") {
    const decomp = createDecompressStream(algo);
    return input.pipe(decomp);
  }

  return input;
}

/**
 * Opens a writable stream for a file or wraps an existing stream with transparent compression.
 */
export function openCompressedWriteStream(
  output: Writable | string,
  options: CompressionOptions = {}
): Writable {
  if (typeof output === "string") {
    const algo = resolveCompression(output, options);
    if (output === "-" || !output) {
      if (algo !== "none") {
        const comp = createCompressStream(algo, options);
        comp.pipe(process.stdout);
        return comp;
      }
      return process.stdout;
    }

    if (algo !== "none") {
      const destStream = createWriteStream(output);
      const comp = createCompressStream(algo, options);
      (comp as any).__destStream = destStream;
      comp.pipe(destStream);
      return comp;
    }
    return createWriteStream(output);
  }

  const algo = resolveCompression(undefined, options);
  if (algo !== "none") {
    const comp = createCompressStream(algo, options);
    (comp as any).__destStream = output;
    comp.pipe(output);
    return comp;
  }

  return output;
}

/**
 * Safely flushes and closes a writable stream, waiting for underlying file descriptors to finish.
 */
export async function closeWritableStream(stream?: Writable): Promise<void> {
  if (!stream) return;
  const destStream = (stream as any).__destStream as Writable | undefined;

  await new Promise<void>((resolve, reject) => {
    if (destStream) {
      let isDone = false;
      const done = (err?: any) => {
        if (isDone) return;
        isDone = true;
        if (err) reject(err);
        else resolve();
      };

      destStream.once("finish", done);
      destStream.once("close", done);
      destStream.once("error", done);
      stream.once("error", done);
      stream.end();
    } else {
      stream.end(() => resolve());
      stream.once("error", reject);
    }
  });
}

/**
 * Opens a readable stream from a file path or stdin ("-"), with transparent decompression.
 */
export function openReadableStream(
  inputPath = "-",
  options: CompressionOptions = {}
): Readable {
  return openDecompressedReadStream(inputPath, options);
}

/**
 * Opens a writable stream to a file path or stdout ("-"), with transparent compression.
 */
export function openWritableStream(
  outputPath = "-",
  options: CompressionOptions = {}
): Writable {
  return openCompressedWriteStream(outputPath, options);
}
