import type { DataStream, Row, TransformFunction } from "../core/types.js";

/**
 * Creates a deterministic PRNG using Mulberry32 algorithm.
 */
function createPrng(seed: number): () => number {
  let s = seed >>> 0;
  return function () {
    let t = (s += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export interface SampleOptions {
  rows: number;
  seed?: number;
}

/**
 * Reservoir sampling transform with bounded O(k) memory.
 */
export function sampleRows(options: SampleOptions): TransformFunction {
  const k = Math.max(1, options.rows);
  const randomFn =
    options.seed !== undefined ? createPrng(options.seed) : Math.random;

  return function (stream: DataStream): DataStream {
    return (async function* () {
      const reservoir: Row[] = [];
      let totalRowsSeen = 0;

      for await (const batch of stream) {
        for (const row of batch.rows) {
          if (totalRowsSeen < k) {
            reservoir.push(row);
          } else {
            // Pick a random integer from 0 to totalRowsSeen
            const j = Math.floor(randomFn() * (totalRowsSeen + 1));
            if (j < k) {
              reservoir[j] = row;
            }
          }
          totalRowsSeen++;
        }
      }

      if (reservoir.length > 0) {
        yield {
          rows: reservoir,
          offset: 0,
        };
      }
    })();
  };
}
