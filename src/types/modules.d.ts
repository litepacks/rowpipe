declare module "fzstd" {
  export class Decompress {
    constructor(ondata: (chunk: Uint8Array, isLast: boolean) => void);
    push(chunk: Uint8Array, isLast?: boolean): void;
  }
  export function decompress(dat: Uint8Array, buf?: Uint8Array): Uint8Array;
}

declare module "@bokuweb/zstd-wasm" {
  export function init(wasmPath?: string): Promise<void>;
  export function compress(buffer: Uint8Array, level?: number): Uint8Array;
  export function decompress(buffer: Uint8Array): Uint8Array;
}
