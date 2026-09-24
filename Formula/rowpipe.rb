class Rowpipe < Formula
  desc "Stream-first tabular data toolkit for CSV, TSV, JSON, XLSX, Parquet, and Markdown"
  homepage "https://github.com/litepacks/rowpipe"
  version "2.10.0"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "#{homepage}/releases/download/v#{version}/rowpipe-darwin-arm64.tar.gz"
      sha256 "REPLACE_WITH_DARWIN_ARM64_SHA256"

      def install
        bin.install "rowpipe"
      end
    end
    if Hardware::CPU.intel?
      url "#{homepage}/releases/download/v#{version}/rowpipe-darwin-x64.tar.gz"
      sha256 "REPLACE_WITH_DARWIN_X64_SHA256"

      def install
        bin.install "rowpipe"
      end
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "#{homepage}/releases/download/v#{version}/rowpipe-linux-x64.tar.gz"
      sha256 "REPLACE_WITH_LINUX_X64_SHA256"

      def install
        bin.install "rowpipe"
      end
    end
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/rowpipe --version")
  end
end
