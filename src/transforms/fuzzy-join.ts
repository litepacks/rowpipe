import type { DataBatch, DataStream, Row, TabularReader, TransformFunction } from "../core/types.js";
import { InvalidArgumentError } from "../core/errors.js";

export type FuzzyMethod = "levenshtein" | "jaro-winkler" | "jaccard" | "soundex";
export type FuzzyJoinType = "inner" | "left" | "right" | "full";

export interface FuzzyJoinOptions {
  rightReader: TabularReader;
  leftKey: string;
  rightKey?: string;
  type?: FuzzyJoinType;
  method?: FuzzyMethod;
  threshold?: number;
  bestMatch?: boolean;
  scoreCol?: string;
  prefixRight?: string;
  suffixRight?: string;
  prefixLeft?: string;
  suffixLeft?: string;
  caseInsensitive?: boolean;
  trim?: boolean;
  batchSize?: number;
}

/**
 * Levenshtein distance between two strings using single-row dynamic programming buffer.
 */
export function levenshteinDistance(a: string, b: string): number {
  if (a === b) return 0;
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;

  const lenA = a.length;
  const lenB = b.length;
  let prevRow = new Int32Array(lenB + 1);
  let currRow = new Int32Array(lenB + 1);

  for (let j = 0; j <= lenB; j++) {
    prevRow[j] = j;
  }

  for (let i = 1; i <= lenA; i++) {
    currRow[0] = i;
    const charA = a.charCodeAt(i - 1);

    for (let j = 1; j <= lenB; j++) {
      const cost = charA === b.charCodeAt(j - 1) ? 0 : 1;
      currRow[j] = Math.min(
        prevRow[j]! + 1, // deletion
        currRow[j - 1]! + 1, // insertion
        prevRow[j - 1]! + cost // substitution
      );
    }

    const tmp = prevRow;
    prevRow = currRow;
    currRow = tmp;
  }

  return prevRow[lenB]!;
}

/**
 * Normalized Levenshtein similarity score in [0.0, 1.0].
 */
export function levenshteinSimilarity(a: string, b: string): number {
  if (a === b) return 1.0;
  const maxLen = Math.max(a.length, b.length);
  if (maxLen === 0) return 1.0;
  const dist = levenshteinDistance(a, b);
  return Math.max(0, 1 - dist / maxLen);
}

/**
 * Jaro similarity score between two strings.
 */
export function jaroSimilarity(s1: string, s2: string): number {
  if (s1 === s2) return 1.0;
  const len1 = s1.length;
  const len2 = s2.length;
  if (len1 === 0 || len2 === 0) return 0.0;

  const matchWindow = Math.floor(Math.max(len1, len2) / 2) - 1;
  const s1Matches = new Uint8Array(len1);
  const s2Matches = new Uint8Array(len2);

  let matches = 0;
  for (let i = 0; i < len1; i++) {
    const start = Math.max(0, i - matchWindow);
    const end = Math.min(i + matchWindow + 1, len2);
    for (let j = start; j < end; j++) {
      if (s2Matches[j]) continue;
      if (s1[i] !== s2[j]) continue;
      s1Matches[i] = 1;
      s2Matches[j] = 1;
      matches++;
      break;
    }
  }

  if (matches === 0) return 0.0;

  let transpositions = 0;
  let k = 0;
  for (let i = 0; i < len1; i++) {
    if (!s1Matches[i]) continue;
    while (!s2Matches[k]) k++;
    if (s1[i] !== s2[k]) transpositions++;
    k++;
  }

  const m = matches;
  const t = transpositions / 2;
  return (m / len1 + m / len2 + (m - t) / m) / 3;
}

/**
 * Jaro-Winkler similarity score with standard scaling factor (p = 0.1).
 */
export function jaroWinklerSimilarity(s1: string, s2: string, p: number = 0.1): number {
  const jaro = jaroSimilarity(s1, s2);
  if (jaro < 0.7) return jaro;

  let prefix = 0;
  const maxPrefix = Math.min(4, Math.min(s1.length, s2.length));
  for (let i = 0; i < maxPrefix; i++) {
    if (s1[i] === s2[i]) prefix++;
    else break;
  }

  return jaro + prefix * p * (1 - jaro);
}

/**
 * Character N-gram Jaccard similarity score in [0.0, 1.0].
 */
export function jaccardSimilarity(s1: string, s2: string, n: number = 2): number {
  if (s1 === s2) return 1.0;
  if (s1.length < n || s2.length < n) {
    return s1 === s2 ? 1.0 : 0.0;
  }

  const ngrams1 = new Set<string>();
  for (let i = 0; i <= s1.length - n; i++) {
    ngrams1.add(s1.slice(i, i + n));
  }

  const ngrams2 = new Set<string>();
  for (let i = 0; i <= s2.length - n; i++) {
    ngrams2.add(s2.slice(i, i + n));
  }

  let intersection = 0;
  for (const gram of ngrams1) {
    if (ngrams2.has(gram)) intersection++;
  }

  const union = ngrams1.size + ngrams2.size - intersection;
  return union === 0 ? 1.0 : intersection / union;
}

/**
 * Soundex phonetic representation (Letter + 3 digits).
 */
export function soundex(s: string): string {
  const clean = s.toUpperCase().replace(/[^A-Z]/g, "");
  if (!clean) return "0000";

  const map: Record<string, string> = {
    B: "1", F: "1", P: "1", V: "1",
    C: "2", G: "2", J: "2", K: "2", Q: "2", S: "2", X: "2", Z: "2",
    D: "3", T: "3",
    L: "4",
    M: "5", N: "5",
    R: "6",
  };

  const firstChar = clean[0]!;
  let result = firstChar;
  let lastCode = map[firstChar] || "0";

  for (let i = 1; i < clean.length && result.length < 4; i++) {
    const char = clean[i]!;
    const code = map[char] || "0";
    if (code !== "0" && code !== lastCode) {
      result += code;
    }
    lastCode = code;
  }

  return (result + "0000").slice(0, 4);
}

/**
 * Computes similarity between two string values based on chosen fuzzy method.
 */
export function computeSimilarity(a: string, b: string, method: FuzzyMethod): number {
  switch (method) {
    case "levenshtein":
      return levenshteinSimilarity(a, b);
    case "jaro-winkler":
      return jaroWinklerSimilarity(a, b);
    case "jaccard":
      return jaccardSimilarity(a, b, 2);
    case "soundex":
      return soundex(a) === soundex(b) ? 1.0 : 0.0;
    default:
      return levenshteinSimilarity(a, b);
  }
}

/**
 * Helper to normalize string for comparison.
 */
function normalizeKey(val: unknown, caseInsensitive: boolean, trim: boolean): string {
  if (val === null || val === undefined) return "";
  let str = String(val);
  if (trim) str = str.trim();
  if (caseInsensitive) str = str.toLowerCase();
  return str;
}

/**
 * Merges a left row and right row, handling key/value formatting and score column.
 */
function mergeFuzzyRows(
  leftRow: Row | null,
  rightRow: Row | null,
  leftKey: string,
  rightKey: string,
  score: number | null,
  options: FuzzyJoinOptions,
  sampleLeftCols?: string[],
  sampleRightCols?: string[]
): Row {
  const result: Row = {};
  const prefixRight = options.prefixRight || "";
  const suffixRight = options.suffixRight !== undefined ? options.suffixRight : (prefixRight ? "" : "_right");
  const prefixLeft = options.prefixLeft || "";
  const suffixLeft = options.suffixLeft || "";

  if (leftRow) {
    for (const [k, v] of Object.entries(leftRow)) {
      const isKey = k === leftKey;
      const outKey = isKey ? k : `${prefixLeft}${k}${suffixLeft}`;
      result[outKey] = v;
    }
  } else if (sampleLeftCols) {
    for (const col of sampleLeftCols) {
      const isKey = col === leftKey;
      const outKey = isKey ? col : `${prefixLeft}${col}${suffixLeft}`;
      result[outKey] = null;
    }
  }

  if (rightRow) {
    for (const [k, v] of Object.entries(rightRow)) {
      if (k === rightKey) {
        if (!leftRow) {
          result[leftKey] = v;
        }
        continue;
      }
      let targetKey = k;
      if (leftRow && k in leftRow) {
        targetKey = `${prefixRight}${k}${suffixRight}`;
      } else if (prefixRight || (suffixRight && suffixRight !== "_right")) {
        targetKey = `${prefixRight}${k}${suffixRight}`;
      }
      result[targetKey] = v;
    }
  } else if (sampleRightCols) {
    for (const col of sampleRightCols) {
      if (col === rightKey) continue;
      let targetKey = col;
      if (leftRow && col in leftRow) {
        targetKey = `${prefixRight}${col}${suffixRight}`;
      } else if (prefixRight || (suffixRight && suffixRight !== "_right")) {
        targetKey = `${prefixRight}${col}${suffixRight}`;
      }
      result[targetKey] = null;
    }
  }

  if (options.scoreCol) {
    result[options.scoreCol] = score !== null ? Math.round(score * 1e4) / 1e4 : null;
  }

  return result;
}

/**
 * Creates a stream transform that performs streaming fuzzy approximate string joins.
 */
export function fuzzyJoinTransform(options: FuzzyJoinOptions): TransformFunction {
  const leftKey = options.leftKey;
  const rightKey = options.rightKey || leftKey;
  const joinType: FuzzyJoinType = options.type || "left";
  const method: FuzzyMethod = options.method || "levenshtein";
  const threshold = options.threshold !== undefined ? options.threshold : 0.75;
  const bestMatch = options.bestMatch !== false;
  const caseInsensitive = options.caseInsensitive !== false;
  const trim = options.trim !== false;
  const batchSize = options.batchSize || 1000;

  return async function* (leftStream: DataStream): DataStream {
    // 1. Buffer and index the right table
    interface RightEntry {
      normKey: string;
      row: Row;
      matched: boolean;
    }

    const rightEntries: RightEntry[] = [];
    let sampleRightCols: string[] | undefined;

    for await (const batch of options.rightReader.read()) {
      for (const row of batch.rows) {
        if (!sampleRightCols) {
          sampleRightCols = Object.keys(row);
        }
        const normKey = normalizeKey(row[rightKey], caseInsensitive, trim);
        rightEntries.push({ normKey, row, matched: false });
      }
    }

    let sampleLeftCols: string[] | undefined;
    let outRows: Row[] = [];
    let offset = 0;

    // 2. Stream through left table and fuzzy match
    for await (const batch of leftStream) {
      for (const leftRow of batch.rows) {
        if (!sampleLeftCols) {
          sampleLeftCols = Object.keys(leftRow);
        }

        const leftNormKey = normalizeKey(leftRow[leftKey], caseInsensitive, trim);

        if (!leftNormKey) {
          // Left row has null / empty key
          if (joinType === "left" || joinType === "full") {
            outRows.push(mergeFuzzyRows(leftRow, null, leftKey, rightKey, null, options, sampleLeftCols, sampleRightCols));
          }
          continue;
        }

        // Search for matches in right table
        interface Candidate {
          entry: RightEntry;
          score: number;
        }

        let candidates: Candidate[] = [];
        for (const entry of rightEntries) {
          if (!entry.normKey) continue;
          const score = computeSimilarity(leftNormKey, entry.normKey, method);
          if (score >= threshold) {
            candidates.push({ entry, score });
          }
        }

        if (candidates.length > 0) {
          if (bestMatch) {
            candidates.sort((a, b) => b.score - a.score);
            const best = candidates[0]!;
            best.entry.matched = true;
            outRows.push(mergeFuzzyRows(leftRow, best.entry.row, leftKey, rightKey, best.score, options, sampleLeftCols, sampleRightCols));
          } else {
            for (const cand of candidates) {
              cand.entry.matched = true;
              outRows.push(mergeFuzzyRows(leftRow, cand.entry.row, leftKey, rightKey, cand.score, options, sampleLeftCols, sampleRightCols));
            }
          }
        } else {
          // No match found
          if (joinType === "left" || joinType === "full") {
            outRows.push(mergeFuzzyRows(leftRow, null, leftKey, rightKey, null, options, sampleLeftCols, sampleRightCols));
          }
        }

        if (outRows.length >= batchSize) {
          yield { rows: outRows, offset };
          offset += outRows.length;
          outRows = [];
        }
      }
    }

    // 3. Emit unmatched right rows for right/full joins
    if (joinType === "right" || joinType === "full") {
      for (const entry of rightEntries) {
        if (!entry.matched) {
          outRows.push(mergeFuzzyRows(null, entry.row, leftKey, rightKey, null, options, sampleLeftCols, sampleRightCols));
          if (outRows.length >= batchSize) {
            yield { rows: outRows, offset };
            offset += outRows.length;
            outRows = [];
          }
        }
      }
    }

    if (outRows.length > 0) {
      yield { rows: outRows, offset };
    }
  };
}
