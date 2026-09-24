class Rowpipe < Formula
  desc "Stream-first tabular data toolkit for CSV, TSV, JSON, XLSX, Parquet, and Markdown"
  homepage "https://github.com/litepacks/rowpipe"
  version "2.10.4"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "#{homepage}/releases/download/v#{version}/rowpipe-darwin-arm64.tar.gz"
      sha256 "4cb114040b4595148f7bd520c82dcc3b7d97dcd8c024d8fd0d9accc10364e6ce"

      def install
        bin.install "rowpipe"
      end
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "#{homepage}/releases/download/v#{version}/rowpipe-linux-x64.tar.gz"
      sha256 "695538ef3ef82e687c3d193b9a8f9b44cbc7640f4686c011d1011cb6fd221ac9"

      def install
        bin.install "rowpipe"
      end
    end
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/rowpipe --version")
  end
end
