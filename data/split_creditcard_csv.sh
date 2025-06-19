#!/bin/bash
# Robustly split creditcard.csv into N parts with header in each, with debug output and error handling

set -e
cd "$(dirname "$0")"

# Debug: show file size
ls -lh creditcard.csv

# Count total lines (including header)
total_lines=$(wc -l < creditcard.csv)
echo "Total lines (including header): $total_lines"

# Calculate lines per split (excluding header)
lines_per_file=$(( (total_lines - 1) / 3 ))
remainder=$(( (total_lines - 1) % 3 ))
echo "Lines per file: $lines_per_file, Remainder: $remainder"

# Extract header
head -n 1 creditcard.csv > header.csv

echo "Running split..."
tail -n +2 creditcard.csv | split -l $lines_per_file - creditcard_tmp_ || { echo "Split command failed!"; exit 1; }

# List split files
echo "Split files created:"
ls -lh creditcard_tmp_* || { echo "No split files created!"; exit 1; }

# Show line counts for split files
for f in creditcard_tmp_*; do
  echo "$f: $(wc -l < "$f") lines"
done

# Combine header with each split to create final files
idx=1
for f in creditcard_tmp_*; do
  cat header.csv "$f" > "creditcard_part${idx}.csv"
  echo "Created creditcard_part${idx}.csv with $(wc -l < "creditcard_part${idx}.csv") lines"
  idx=$((idx+1))
done

# Clean up temporary files
rm creditcard_tmp_*
rm header.csv

echo "\nValidating split..."
# Count lines in the original file
orig_lines=$(wc -l < creditcard.csv)
echo "Original file lines: $orig_lines"

# Count lines in each split part and sum
total=0
for f in creditcard_part*.csv; do
  lines=$(wc -l < "$f")
  # Subtract 1 for the header
  data_lines=$((lines - 1))
  total=$((total + data_lines))
done
# Add 1 for the header in the original file
total=$((total + 1))
echo "Total lines in all parts (including one header): $total"

if [ "$orig_lines" -eq "$total" ]; then
  echo "Validation successful: Split files match the original file."
else
  echo "Validation failed: Line counts do not match!"
fi
