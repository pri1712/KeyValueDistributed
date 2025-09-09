#!/bin/bash
file1=$1
file2=$2

# Extract pairs like "1 6636" or "0 <nil>", remove brackets and braces
normalize() {
  sed 's/[][]//g; s/[{}]//g' "$1" | tr -s ' ' '\n' | paste - - | sort
}

normalize "$file1" > /tmp/arr1.txt
normalize "$file2" > /tmp/arr2.txt

echo "Differences (in file1 not in file2):"
comm -23 /tmp/arr1.txt /tmp/arr2.txt

echo "Differences (in file2 not in file1):"
comm -13 /tmp/arr1.txt /tmp/arr2.txt