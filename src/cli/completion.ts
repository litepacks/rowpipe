export function generateZshCompletion(): string {
  return `#compdef rowpipe

_rowpipe() {
  local -a commands
  commands=(
    'inspect:Inspect format, row count, columns, and sheet summary'
    'convert:Stream convert tabular datasets across formats'
    'schema:Infer column types, nullability, and semantic annotations'
    'stats:Compute streaming statistics (Welford numeric stats and HLL distinct counts)'
    'select:Select a subset of columns in stream'
    'rename:Rename columns in stream'
    'cast:Cast column types in stream'
    'filter:Filter rows using safe expression engine'
    'map:Transform and derive columns per row'
    'reduce:Aggregate and group dataset'
    'sample:Reservoir sample rows with bounded memory'
    'validate:Validate stream against schema definition'
    'diff:Compare two tabular datasets by key'
    'files:Stream files and directories as tabular records'
    'diff-files:Compare two directories as streaming tabular datasets'
    'limit:Limit stream to first N rows'
    'offset:Skip first N rows in stream'
    'head:Emit first N rows of stream'
    'tail:Emit last N rows of stream'
    'sort:Sort stream by columns'
    'top:Retain top-N rows using bounded-memory binary heap'
    'unique:Deduplicate rows by key columns'
    'count:Fast streaming row count or group-by counts'
    'group:Group dataset and compute aggregations'
    'db:Stream tabular data from or to databases'
    'join:Join two tabular streams with hash join'
    'window:Apply sliding window and rolling calculations'
    'profile:Profile dataset quality and distributions'
    'clean:Clean and sanitize dataset'
    'explain:Visualize execution plan and optimizations'
    'view:Interactive terminal data viewer'
    'explode:Expand array or delimited string column into multiple rows'
    'flatten:Flatten nested JSON objects into dot-notated columns'
    'freq:Compute frequency distribution and value counts'
    'plot:Render horizontal ASCII/Unicode bar charts and histograms'
    'chart:Alias for plot'
    'table:Render dataset as pretty terminal grid table'
    'completion:Generate shell autocompletion script'
    'partition:Partition stream into dynamic files based on column values'
    'split:Split stream into sequential chunk files'
    'timeseries:Resample timeseries data and fill missing gaps'
    'corr:Compute Pearson correlation matrix across numeric columns'
    'quantiles:Compute streaming quantiles, percentiles, and IQR'
    'percentiles:Alias for quantiles'
    'outliers:Detect and filter statistical outliers and anomalies'
    'anomalies:Alias for outliers'
    'crosstab:Compute 2D contingency table and Chi-Square statistic'
    'regression:Compute Ordinary Least Squares linear regression'
    'trend:Alias for regression'
    'rfm:Compute RFM customer segmentation and scoring'
    'cohort:Compute user retention cohort matrix'
    'funnel:Compute conversion funnel and drop-off rates'
    'abtest:Compute A/B test statistical significance'
    'pareto:Compute 80/20 Pareto distribution and ABC classification'
    'technical:Compute financial technical indicators'
    'indicators:Alias for technical'
    'cluster:Perform streaming Mini-Batch K-Means clustering'
    'kmeans:Alias for cluster'
    'entropy:Compute Shannon entropy and feature importance'
    'importance:Alias for entropy'
    'ngrams:Extract N-Gram word phrases and frequency'
    'tokens:Alias for ngrams'
    'pivot:Create multi-dimensional pivot table'
    'pivot-table:Alias for pivot'
    'crosstab-pivot:Alias for pivot'
    'unpivot:Unpivot wide dataset into long format'
    'melt:Alias for unpivot'
    'wide-to-long:Alias for unpivot'
    'fuzzy-join:Fuzzy approximate string matching join'
    'fuzzyjoin:Alias for fuzzy-join'
    'fuzzy:Alias for fuzzy-join'
    'concat:Stream concatenate multiple tabular files'
    'stack:Alias for concat'
    'union-all:Alias for concat'
    'generate:Stream generate synthetic tabular data'
    'mock:Alias for generate'
    'fake:Alias for generate'
    'synth:Alias for generate'
    'mask:Redact and mask sensitive PII fields'
    'anonymize:Alias for mask'
    'redact:Alias for mask'
    'test:Run data quality tests and assertions'
    'assert:Alias for test'
    'check:Alias for test'
    'serve:Serve dataset via HTTP REST API'
    'api:Alias for serve'
    'http:Alias for serve'
    'report:Generate HTML data health report'
    'summary-html:Alias for report'
    'fetch:Stream tabular data from remote REST API'
    'http-get:Alias for fetch'
    'curl-stream:Alias for fetch'
  )

  _arguments -C \
    '1: :->command' \
    '*:: :->args'

  case $state in
    command)
      _describe -t commands 'rowpipe command' commands
      ;;
    args)
      case $words[1] in
        convert|inspect|schema|stats|select|filter|map|reduce|diff|table|view|plot|chart|freq|explode|flatten|partition|split|timeseries|corr|quantiles|percentiles|outliers|anomalies|crosstab|regression|trend|rfm|cohort|funnel|abtest|pareto|technical|indicators|cluster|kmeans|entropy|importance|ngrams|tokens|pivot|pivot-table|crosstab-pivot|unpivot|melt|wide-to-long|fuzzy-join|fuzzyjoin|fuzzy|concat|stack|union-all|generate|mock|fake|synth|mask|anonymize|redact|test|assert|check|serve|api|http|report|summary-html|fetch|http-get|curl-stream)
          _files
          ;;
        completion)
          local -a shells
          shells=('zsh:Zsh completion' 'bash:Bash completion' 'fish:Fish completion')
          _describe -t shells 'shell' shells
          ;;
        *)
          _files
          ;;
      esac
      ;;
  esac
}

_rowpipe "$@"
`;
}

export function generateBashCompletion(): string {
  return `#!/usr/bin/env bash

_rowpipe_completions() {
  local cur prev commands
  cur="\${COMP_WORDS[COMP_CWORD]}"
  prev="\${COMP_WORDS[COMP_CWORD-1]}"

  commands="inspect convert schema stats select rename cast filter map reduce sample validate diff files diff-files limit offset head tail sort top unique count group db join window profile clean explain view explode flatten freq plot chart table completion partition split timeseries corr quantiles percentiles outliers anomalies crosstab regression trend rfm cohort funnel abtest pareto technical indicators cluster kmeans entropy importance ngrams tokens pivot pivot-table crosstab-pivot unpivot melt wide-to-long fuzzy-join fuzzyjoin fuzzy concat stack union-all generate mock fake synth mask anonymize redact test assert check serve api http report summary-html fetch http-get curl-stream"

  if [ $COMP_CWORD -eq 1 ]; then
    COMPREPLY=( $(compgen -W "$commands" -- "$cur") )
    return 0
  fi

  case "$prev" in
    --from|--to)
      COMPREPLY=( $(compgen -W "csv tsv psv json jsonl parquet xlsx markdown table db" -- "$cur") )
      return 0
      ;;
    completion)
      COMPREPLY=( $(compgen -W "zsh bash fish" -- "$cur") )
      return 0
      ;;
    *)
      COMPREPLY=( $(compgen -f -- "$cur") )
      return 0
      ;;
  esac
}

complete -F _rowpipe_completions rowpipe
`;
}

export function generateFishCompletion(): string {
  return `# Fish completion for rowpipe
complete -c rowpipe -f

set -l commands inspect convert schema stats select rename cast filter map reduce sample validate diff files diff-files limit offset head tail sort top unique count group db join window profile clean explain view explode flatten freq plot chart table completion partition split timeseries corr quantiles percentiles outliers anomalies crosstab regression trend rfm cohort funnel abtest pareto technical indicators cluster kmeans entropy importance ngrams tokens pivot pivot-table crosstab-pivot unpivot melt wide-to-long fuzzy-join fuzzyjoin fuzzy concat stack union-all generate mock fake synth mask anonymize redact test assert check serve api http report summary-html fetch http-get curl-stream

for cmd in $commands
  complete -c rowpipe -n "not __fish_seen_subcommand_from $commands" -a $cmd
end

complete -c rowpipe -n "__fish_seen_subcommand_from completion" -a "zsh bash fish"
complete -c rowpipe -l from -d "Input format" -a "csv tsv psv json jsonl parquet xlsx"
complete -c rowpipe -l to -d "Output format" -a "csv tsv psv json jsonl parquet xlsx markdown table"
complete -c rowpipe -l json -d "Output JSON"
complete -c rowpipe -l help -s h -d "Show help"
complete -c rowpipe -l version -s V -d "Show version"
`;
}

export function generateCompletion(shell: string): string {
  switch (shell.toLowerCase()) {
    case "zsh":
      return generateZshCompletion();
    case "bash":
      return generateBashCompletion();
    case "fish":
      return generateFishCompletion();
    default:
      throw new Error(`Unsupported shell: "${shell}". Supported shells: zsh, bash, fish`);
  }
}
