import { levenshteinDistance } from "../transforms/fuzzy-join.js";

export { levenshteinDistance };

/**
 * Finds the closest matching candidate for a given target string using Levenshtein distance.
 * Returns null if no candidate is within maxDistance.
 */
export function findClosestMatch(
  target: string,
  candidates: string[],
  maxDistance = 3
): string | null {
  if (!target || candidates.length === 0) return null;

  let closest: string | null = null;
  let minDistance = maxDistance + 1;
  const normTarget = target.trim().toLowerCase();

  for (const candidate of candidates) {
    const normCandidate = candidate.trim().toLowerCase();
    const dist = levenshteinDistance(normTarget, normCandidate);
    if (dist < minDistance) {
      minDistance = dist;
      closest = candidate;
    }
  }

  return closest;
}
