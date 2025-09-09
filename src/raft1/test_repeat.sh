#!/bin/bash
num_runs=$1
cmd=$2

for ((i=1; i<=num_runs; i++)); do
  echo "========== Run $i =========="
  output=$(go test -run "$cmd" 2>&1)
  echo "$output"
  failed=$(echo "$output" | grep -E "FAIL\s+[a-zA-Z0-9_/]+\.Test[^\s]+")

  if [ -n "$failed" ]; then
    echo "Run $i failed tests:"
    echo "$failed"
  else
    echo "Run $i passed all 3B tests"
  fi
done