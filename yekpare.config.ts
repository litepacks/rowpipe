import { defineConfig } from "yekpare";

export default defineConfig({
  // CLI Entry point
  entry: "dist/cli/index.js",

  // Executable binary name
  name: "rowpipe",

  // Target platforms for distribution
  targets: [
    "darwin-arm64",
    "darwin-x64",
    "linux-x64",
    "linux-arm64",
    "win32-x64",
  ],

  // Assets to embed inside the standalone executable
  assets: [],

  // Bundler configuration
  bundle: {
    minify: true,
    sourcemap: false,
  },

  // Binary optimizations
  binary: {
    strip: true,
  },

  // Release format
  release: {
    format: "tar.gz",
    checksum: "sha256",
  },

  // SEA configuration
  sea: {
    useCodeCache: true,
    disableExperimentalSEAWarning: true,
  },
});

