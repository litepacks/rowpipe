import { createHash } from "node:crypto";
import { createReadStream, promises as fsPromises } from "node:fs";
import { pipeline } from "node:stream/promises";
import { FileSystemError } from "../core/errors.js";
import type { HashAlgorithm } from "./types.js";

/**
 * Computes streaming cryptographic hash of a file.
 */
export async function computeFileHash(
  filePath: string,
  algorithm: HashAlgorithm = "sha256"
): Promise<string> {
  if (algorithm === "fast") {
    return computeFastFileHash(filePath);
  }

  try {
    const hashStream = createHash(algorithm);
    const fileStream = createReadStream(filePath);
    await pipeline(fileStream, hashStream);
    return hashStream.digest("hex");
  } catch (err: unknown) {
    throw new FileSystemError(`Failed to compute ${algorithm} hash for "${filePath}"`, {
      path: filePath,
      operation: "hash",
      cause: err instanceof Error ? err : String(err),
    });
  }
}

/**
 * Computes a fast non-cryptographic fingerprint using file stat + partial content sampling.
 */
export async function computeFastFileHash(filePath: string): Promise<string> {
  try {
    const stat = await fsPromises.stat(filePath);
    const size = stat.size;
    const mtimeMs = stat.mtimeMs;

    if (size === 0) {
      return `fast:0:${mtimeMs}:empty`;
    }

    const hash = createHash("sha256");
    hash.update(`size:${size}|mtime:${mtimeMs}|`);

    // Sample up to 4KB from start and end for fast fingerprinting
    const sampleSize = Math.min(4096, size);
    const fd = await fsPromises.open(filePath, "r");

    try {
      const buffer = Buffer.alloc(sampleSize);
      // Read header
      await fd.read(buffer, 0, sampleSize, 0);
      hash.update(buffer);

      if (size > sampleSize * 2) {
        // Read footer if large enough
        const footerBuffer = Buffer.alloc(sampleSize);
        await fd.read(footerBuffer, 0, sampleSize, size - sampleSize);
        hash.update(footerBuffer);
      }
    } finally {
      await fd.close();
    }

    return `fast:${hash.digest("hex").slice(0, 16)}`;
  } catch (err: unknown) {
    throw new FileSystemError(`Failed to compute fast hash for "${filePath}"`, {
      path: filePath,
      operation: "hash",
      cause: err instanceof Error ? err : String(err),
    });
  }
}
