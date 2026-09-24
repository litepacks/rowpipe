import type { Row, DataBatch } from "../core/types.js";

export interface NgramOptions {
  col?: string;
  n?: number; // N-gram size: 1, 2, 3... (default: 2)
  top?: number; // Top N results (default: 20)
  stopwords?: boolean;
  minFreq?: number;
  caseSensitive?: boolean;
}

export interface NgramFrequencyItem {
  ngram: string;
  count: number;
  percentage: number;
  bar: string;
}

export interface NgramResult {
  n: number;
  column: string;
  total_ngrams: number;
  unique_ngrams: number;
  items: NgramFrequencyItem[];
  formattedRows: Row[];
}

const DEFAULT_STOPWORDS = new Set([
  "a", "about", "above", "after", "again", "against", "all", "am", "an", "and",
  "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being",
  "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't",
  "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during",
  "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't",
  "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here",
  "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i",
  "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
  "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself",
  "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought",
  "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she",
  "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such",
  "than", "that", "that's", "the", "their", "theirs", "them", "themselves",
  "then", "there", "there's", "these", "they", "they'd", "they'll", "they're",
  "they've", "this", "those", "through", "to", "too", "under", "until", "up",
  "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were",
  "weren't", "what", "what's", "when", "when's", "where", "where's", "which",
  "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
  "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours",
  "yourself", "yourselves"
]);

function tokenizeText(text: string, caseSensitive: boolean, removeStopwords: boolean): string[] {
  const clean = caseSensitive ? text : text.toLowerCase();
  const words = clean
    .replace(/[^\p{L}\p{N}\s_]/gu, " ")
    .split(/\s+/)
    .map((w) => w.trim())
    .filter((w) => w.length > 0);

  if (removeStopwords) {
    return words.filter((w) => !DEFAULT_STOPWORDS.has(w.toLowerCase()));
  }
  return words;
}

function renderBar(pct: number, width = 15): string {
  const filled = Math.max(0, Math.min(width, Math.round((pct / 100) * width)));
  const empty = width - filled;
  return "█".repeat(filled) + "░".repeat(empty);
}

export async function computeNgrams(
  batchIterator: AsyncIterable<DataBatch>,
  options: NgramOptions = {}
): Promise<NgramResult> {
  const n = Math.max(1, options.n ?? 2);
  const top = options.top ?? 20;
  const minFreq = options.minFreq ?? 1;
  const removeStopwords = options.stopwords ?? false;
  const caseSensitive = options.caseSensitive ?? false;

  let targetCol = options.col;
  const freqMap = new Map<string, number>();
  let totalNgrams = 0;

  for await (const batch of batchIterator) {
    if (batch.rows.length === 0) continue;

    if (!targetCol) {
      const firstRow = batch.rows[0]!;
      const keys = Object.keys(firstRow);
      targetCol =
        keys.find((k) => /(text|title|name|description|body|review|content|message|summary)/i.test(k)) ??
        keys.find((k) => typeof firstRow[k] === "string") ??
        keys[0];
    }

    for (const row of batch.rows) {
      const rawVal = row[targetCol!];
      if (rawVal === null || rawVal === undefined || rawVal === "") continue;
      const text = String(rawVal);

      const tokens = tokenizeText(text, caseSensitive, removeStopwords);
      if (tokens.length < n) continue;

      for (let i = 0; i <= tokens.length - n; i++) {
        const gram = tokens.slice(i, i + n).join(" ");
        freqMap.set(gram, (freqMap.get(gram) || 0) + 1);
        totalNgrams++;
      }
    }
  }

  if (!targetCol) {
    throw new Error("No text column found for N-gram extraction.");
  }

  // Filter and sort top results
  const sorted = Array.from(freqMap.entries())
    .filter(([, count]) => count >= minFreq)
    .sort((a, b) => b[1] - a[1])
    .slice(0, top);

  const maxCount = sorted.length > 0 ? sorted[0]![1] : 1;

  const items: NgramFrequencyItem[] = sorted.map(([ngram, count]) => {
    const pct = totalNgrams > 0 ? (count / totalNgrams) * 100 : 0;
    const relativePct = (count / maxCount) * 100;
    return {
      ngram,
      count,
      percentage: Math.round(pct * 100) / 100,
      bar: renderBar(relativePct, 15),
    };
  });

  const formattedRows: Row[] = items.map((it, idx) => ({
    rank: idx + 1,
    ngram: it.ngram,
    count: it.count,
    share: `${it.percentage.toFixed(2)}%`,
    distribution: it.bar,
  }));

  return {
    n,
    column: targetCol,
    total_ngrams: totalNgrams,
    unique_ngrams: freqMap.size,
    items,
    formattedRows,
  };
}
