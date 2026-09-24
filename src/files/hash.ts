import { createHash } from "node:crypto";
import { createReadStream, promises as fsPromises, Stats } from "node:fs";
import { pipeline } from "node:stream/promises";
import { FileSystemError } from "../core/errors.js";
import type { HashAlgorithm } from "./types.js";

/**
 * Computes streaming cryptographic hash of a file.
 */
export async function computeFileHash(
  filePath: string,
  algorithm: HashAlgorithm = "sha256",
  stat?: Stats | { size: number; mtimeMs: number }
): Promise<string> {
  if (algorithm === "fast") {
    return computeFastFileHash(filePath, stat);
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
export async function computeFastFileHash(
  filePath: string,
  existingStat?: Stats | { size: number; mtimeMs: number }
): Promise<string> {
  try {
    const stat = existingStat ?? (await fsPromises.stat(filePath));
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
      const buffer = Buffer.allocUnsafe(sampleSize);
      // Read header
      const { bytesRead: headerRead } = await fd.read(buffer, 0, sampleSize, 0);
      hash.update(buffer.subarray(0, headerRead));

      if (size > sampleSize * 2) {
        // Read footer if large enough
        const { bytesRead: footerRead } = await fd.read(buffer, 0, sampleSize, size - sampleSize);
        hash.update(buffer.subarray(0, footerRead));
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

