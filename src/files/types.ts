export type FileType = "file" | "directory" | "symlink";

export type FileTypeFilter = "file" | "directory" | "symlink" | "all";

export type HashAlgorithm = "sha256" | "sha1" | "md5" | "fast";

export type FileCategory =
  | "image"
  | "video"
  | "audio"
  | "archive"
  | "document"
  | "data"
  | "code"
  | "executable"
  | "font"
  | "other";

export interface FileRecord {
  path: string;
  relative_path: string;
  name: string;
  basename: string;
  extension: string;
  directory: string;
  type: FileType;
  size: number;
  created_at: string;
  modified_at: string;
  accessed_at?: string;
  mode: number;
  is_file: boolean;
  is_directory: boolean;
  is_symlink: boolean;
  hash?: string;
  mime?: string;
  category?: FileCategory;
  size_human?: string;
  [key: string]: unknown;
}

export interface FileSystemReaderOptions {
  root: string;
  recursive?: boolean;
  maxDepth?: number;
  include?: string[];
  exclude?: string[];
  followSymlinks?: boolean;
  hidden?: boolean;
  type?: FileTypeFilter;
  hash?: false | HashAlgorithm;
  mime?: boolean;
  category?: boolean;
  human?: boolean;
  concurrency?: number;
  onError?: "fail" | "abort" | "skip" | "log";
  badRowsLog?: string;
  deterministic?: boolean;
  batchSize?: number;
}

export interface FileMetadataEnricher {
  name: string;
  enrich(file: FileRecord): Promise<Record<string, unknown>>;
}
