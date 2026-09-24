/**
 * Zero-dependency, high-performance glob compiler and matcher.
 */

/**
 * Expands brace patterns like `*.{jpg,png}` into `["*.jpg", "*.png"]`.
 */
export function expandBracePatterns(pattern: string): string[] {
  const match = pattern.match(/\{([^}]+)\}/);
  if (!match) return [pattern];
  const prefix = pattern.slice(0, match.index);
  const suffix = pattern.slice(match.index! + match[0].length);
  const options = match[1]!.split(",").map((s) => s.trim());
  const results: string[] = [];
  for (const opt of options) {
    const expanded = `${prefix}${opt}${suffix}`;
    results.push(...expandBracePatterns(expanded));
  }
  return results;
}

/**
 * Splits comma-separated patterns only outside of curly braces {}.
 */
function splitTopLevelCommas(input: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = "";
  for (let i = 0; i < input.length; i++) {
    const c = input[i];
    if (c === "{") {
      depth++;
      current += c;
    } else if (c === "}") {
      depth = Math.max(0, depth - 1);
      current += c;
    } else if (c === "," && depth === 0) {
      if (current.trim().length > 0) {
        parts.push(current.trim());
      }
      current = "";
    } else {
      current += c;
    }
  }
  if (current.trim().length > 0) {
    parts.push(current.trim());
  }
  return parts;
}

/**
 * Normalizes file path to forward slashes without leading `./`.
 */
export function normalizeGlobPath(filePath: string): string {
  let normalized = filePath.replace(/\\/g, "/");
  while (normalized.startsWith("./")) {
    normalized = normalized.slice(2);
  }
  return normalized;
}

/**
 * Converts a single glob pattern into a regular expression.
 */
export function globToRegex(pattern: string): RegExp {
  const cleanPattern = normalizeGlobPath(pattern);

  // If pattern does not contain a slash, treat it as matching either at root or anywhere in tree
  const hasSlash = cleanPattern.includes("/");

  let regexStr = "";
  let i = 0;
  const len = cleanPattern.length;

  while (i < len) {
    const c = cleanPattern[i];
    if (c === undefined) break;

    if (c === "*" && cleanPattern[i + 1] === "*") {
      // '**'
      i += 2;
      if (cleanPattern[i] === "/") {
        i += 1;
        regexStr += "(?:.*?/)?";
      } else {
        regexStr += ".*";
      }
    } else if (c === "*") {
      regexStr += "[^/]*";
      i += 1;
    } else if (c === "?") {
      regexStr += "[^/]";
      i += 1;
    } else if ("()+^${}|[].\\".includes(c)) {
      regexStr += `\\${c}`;
      i += 1;
    } else {
      regexStr += c;
      i += 1;
    }
  }

  if (!hasSlash) {
    return new RegExp(`^(?:.*?/)?${regexStr}$`, "i");
  }

  return new RegExp(`^${regexStr}$`, "i");
}

export type GlobMatcher = (path: string) => boolean;

/**
 * Creates a composite matcher function from an array of glob strings.
 */
export function createGlobMatcher(patterns?: string[] | string): GlobMatcher {
  if (!patterns) {
    return () => true;
  }

  const rawPatterns = Array.isArray(patterns) ? patterns : [patterns];
  const normalizedPatterns = rawPatterns
    .flatMap(splitTopLevelCommas)
    .map((p) => p.trim())
    .filter((p) => p.length > 0)
    .flatMap(expandBracePatterns);

  if (normalizedPatterns.length === 0) {
    return () => true;
  }

  const regexes = normalizedPatterns.map(globToRegex);

  return (filePath: string) => {
    const normalized = normalizeGlobPath(filePath);
    return regexes.some((re) => re.test(normalized));
  };
}
