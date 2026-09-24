import type { FileCategory } from "./types.js";

interface MimeEntry {
  mime: string;
  category: FileCategory;
}

const MIME_MAP: Record<string, MimeEntry> = {
  // Tabular & Data
  csv: { mime: "text/csv", category: "data" },
  tsv: { mime: "text/tab-separated-values", category: "data" },
  psv: { mime: "text/plain", category: "data" },
  tab: { mime: "text/tab-separated-values", category: "data" },
  json: { mime: "application/json", category: "data" },
  jsonl: { mime: "application/x-ndjson", category: "data" },
  ndjson: { mime: "application/x-ndjson", category: "data" },
  ldjson: { mime: "application/x-ndjson", category: "data" },
  parquet: { mime: "application/vnd.apache.parquet", category: "data" },
  pq: { mime: "application/vnd.apache.parquet", category: "data" },
  sql: { mime: "application/sql", category: "data" },
  sqlite: { mime: "application/vnd.sqlite3", category: "data" },
  db: { mime: "application/octet-stream", category: "data" },
  xml: { mime: "application/xml", category: "data" },
  yaml: { mime: "application/yaml", category: "data" },
  yml: { mime: "application/yaml", category: "data" },
  toml: { mime: "application/toml", category: "data" },

  // Spreadsheets & Office Documents
  xlsx: { mime: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", category: "document" },
  xlsm: { mime: "application/vnd.ms-excel.sheet.macroEnabled.12", category: "document" },
  xls: { mime: "application/vnd.ms-excel", category: "document" },
  ods: { mime: "application/vnd.oasis.opendocument.spreadsheet", category: "document" },
  pdf: { mime: "application/pdf", category: "document" },
  docx: { mime: "application/vnd.openxmlformats-officedocument.wordprocessingml.document", category: "document" },
  doc: { mime: "application/msword", category: "document" },
  pptx: { mime: "application/vnd.openxmlformats-officedocument.presentationml.presentation", category: "document" },
  ppt: { mime: "application/vnd.ms-powerpoint", category: "document" },
  txt: { mime: "text/plain", category: "document" },
  md: { mime: "text/markdown", category: "document" },
  markdown: { mime: "text/markdown", category: "document" },
  rtf: { mime: "application/rtf", category: "document" },
  epub: { mime: "application/epub+zip", category: "document" },

  // Images
  png: { mime: "image/png", category: "image" },
  jpg: { mime: "image/jpeg", category: "image" },
  jpeg: { mime: "image/jpeg", category: "image" },
  webp: { mime: "image/webp", category: "image" },
  gif: { mime: "image/gif", category: "image" },
  svg: { mime: "image/svg+xml", category: "image" },
  avif: { mime: "image/avif", category: "image" },
  bmp: { mime: "image/bmp", category: "image" },
  ico: { mime: "image/x-icon", category: "image" },
  tiff: { mime: "image/tiff", category: "image" },
  tif: { mime: "image/tiff", category: "image" },
  heic: { mime: "image/heic", category: "image" },

  // Video
  mp4: { mime: "video/mp4", category: "video" },
  mkv: { mime: "video/x-matroska", category: "video" },
  webm: { mime: "video/webm", category: "video" },
  avi: { mime: "video/x-msvideo", category: "video" },
  mov: { mime: "video/quicktime", category: "video" },
  wmv: { mime: "video/x-ms-wmv", category: "video" },
  flv: { mime: "video/x-flv", category: "video" },

  // Audio
  mp3: { mime: "audio/mpeg", category: "audio" },
  wav: { mime: "audio/wav", category: "audio" },
  flac: { mime: "audio/flac", category: "audio" },
  aac: { mime: "audio/aac", category: "audio" },
  ogg: { mime: "audio/ogg", category: "audio" },
  m4a: { mime: "audio/mp4", category: "audio" },

  // Archives & Compression
  zip: { mime: "application/zip", category: "archive" },
  tar: { mime: "application/x-tar", category: "archive" },
  gz: { mime: "application/gzip", category: "archive" },
  gzip: { mime: "application/gzip", category: "archive" },
  tgz: { mime: "application/gzip", category: "archive" },
  bz2: { mime: "application/x-bzip2", category: "archive" },
  xz: { mime: "application/x-xz", category: "archive" },
  zst: { mime: "application/zstd", category: "archive" },
  "7z": { mime: "application/x-7z-compressed", category: "archive" },
  rar: { mime: "application/vnd.rar", category: "archive" },

  // Code & Source
  js: { mime: "text/javascript", category: "code" },
  mjs: { mime: "text/javascript", category: "code" },
  cjs: { mime: "text/javascript", category: "code" },
  ts: { mime: "text/typescript", category: "code" },
  mts: { mime: "text/typescript", category: "code" },
  cts: { mime: "text/typescript", category: "code" },
  jsx: { mime: "text/javascript", category: "code" },
  tsx: { mime: "text/typescript", category: "code" },
  html: { mime: "text/html", category: "code" },
  htm: { mime: "text/html", category: "code" },
  css: { mime: "text/css", category: "code" },
  scss: { mime: "text/x-scss", category: "code" },
  sass: { mime: "text/x-sass", category: "code" },
  less: { mime: "text/less", category: "code" },
  py: { mime: "text/x-python", category: "code" },
  rs: { mime: "text/x-rust", category: "code" },
  go: { mime: "text/x-go", category: "code" },
  c: { mime: "text/x-c", category: "code" },
  cpp: { mime: "text/x-c++", category: "code" },
  h: { mime: "text/x-c", category: "code" },
  hpp: { mime: "text/x-c++", category: "code" },
  java: { mime: "text/x-java-source", category: "code" },
  kt: { mime: "text/x-kotlin", category: "code" },
  swift: { mime: "text/x-swift", category: "code" },
  rb: { mime: "text/x-ruby", category: "code" },
  php: { mime: "application/x-httpd-php", category: "code" },
  sh: { mime: "application/x-sh", category: "code" },
  bash: { mime: "application/x-sh", category: "code" },
  zsh: { mime: "application/x-sh", category: "code" },

  // Fonts
  woff: { mime: "font/woff", category: "font" },
  woff2: { mime: "font/woff2", category: "font" },
  ttf: { mime: "font/ttf", category: "font" },
  otf: { mime: "font/otf", category: "font" },
};

/**
 * Returns MIME type and Category for a given file extension or name.
 */
export function inferMimeAndCategory(extensionOrName: string): { mime: string; category: FileCategory } {
  let ext = extensionOrName.toLowerCase();
  const dotIndex = ext.lastIndexOf(".");
  if (dotIndex !== -1) {
    ext = ext.slice(dotIndex + 1);
  }

  const entry = MIME_MAP[ext];
  if (entry) {
    return entry;
  }

  return {
    mime: "application/octet-stream",
    category: "other",
  };
}
