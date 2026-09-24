import { compileExpression, compileValueExpression, Lexer, Parser, ASTNode } from "../src/transforms/expression.js";
import { tryCompileJITPredicate, tryCompileJITValue } from "../src/transforms/jit-compiler.js";
import { formatNumber } from "../src/utils/formatting.js";
import type { Row } from "../src/core/types.js";

async function main() {
  const TOTAL_ROWS = 1_000_000;
  console.log("======================================================");
  console.log("    Rowpipe Expression JIT vs AST Benchmark           ");
  console.log("======================================================");
  console.log(`Generating synthetic dataset (${formatNumber(TOTAL_ROWS)} rows)...`);

  const dataset: Row[] = new Array(TOTAL_ROWS);
  for (let i = 0; i < TOTAL_ROWS; i++) {
    dataset[i] = {
      id: i,
      name: `User_${i}`,
      age: 18 + (i % 60),
      country: i % 3 === 0 ? "TR" : i % 3 === 1 ? "US" : "DE",
      revenue: (i % 500) * 2.5,
      active: i % 2 === 0,
    };
  }
  console.log("Dataset ready.\n");

  const testExpressions = [
    { name: "Simple Filter", expr: "age > 30" },
    { name: "Compound Filter (AND + OR)", expr: "age >= 25 && country == 'TR' || revenue > 500" },
    { name: "String & Equality Filter", expr: "country == 'US' && active == true" },
  ];

  for (const { name, expr } of testExpressions) {
    console.log(`--- Benchmark: ${name} ---`);
    console.log(`Expression: "${expr}"`);

    // 1. Raw AST Evaluation (simulate unoptimized tree walking)
    const lexer = new Lexer(expr);
    const tokens = lexer.tokenize();
    const parser = new Parser(tokens);
    const ast = parser.parse();

    // JIT compiled function
    const jitFn = tryCompileJITPredicate(ast)!;
    // Standard compiled function
    const standardFn = compileExpression(expr);

    // Warm-up
    for (let i = 0; i < 10000; i++) {
      jitFn(dataset[i]!);
    }

    // Benchmark JIT
    const startJIT = Date.now();
    let matchCountJIT = 0;
    for (let i = 0; i < TOTAL_ROWS; i++) {
      if (jitFn(dataset[i]!)) {
        matchCountJIT++;
      }
    }
    const elapsedJIT = Date.now() - startJIT;
    const throughputJIT = Math.round((TOTAL_ROWS / (elapsedJIT || 1)) * 1000);

    console.log(`[JIT Native Function]:  ${elapsedJIT} ms | Throughput: ${formatNumber(throughputJIT)} rows/sec (Matched: ${formatNumber(matchCountJIT)})`);
    console.log("");
  }

  console.log("======================================================");
  console.log("            JIT BENCHMARK COMPLETED                   ");
  console.log("======================================================");
}

main().catch(console.error);
