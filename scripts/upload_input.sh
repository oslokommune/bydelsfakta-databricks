#!/usr/bin/env bash

# Upload Excel files from `input` to the `uploads` Volume.
#
# Each file is placed in a subdirectory matching its name (without extension),
# which is what the jobs expect.
#
# Usage:
#   ./scripts/upload_input.sh <catalog> <schema> <profile>
#
# Example:
#   ./scripts/upload_input.sh dig_bydelsfakta_dev_green bronze_default BYDELSFAKTA_DEV

set -euo pipefail

catalog="${1:?Usage: $0 <catalog> <schema> <profile>}"
schema="${2:?Usage: $0 <catalog> <schema> <profile>}"
profile="${3:?Usage: $0 <catalog> <schema> <profile>}"
base="dbfs:/Volumes/${catalog}/${schema}/uploads"
input_dir="$(cd "$(dirname "$0")/.." && pwd)/input"

if [ ! -d "$input_dir" ]; then
    echo "Error: ${input_dir} not found"
    exit 1
fi

count=0
for file in "$input_dir"/*.xlsx; do
    [ -f "$file" ] || continue
    name="$(basename "$file" .xlsx)"
    target="${base}/${name}/$(basename "$file")"
    echo -n "Uploading ${name}.xlsx ... "
    databricks fs cp "$file" "$target" --overwrite -p "${profile}"
    count=$((count + 1))
done

echo "Done. Uploaded ${count} files."
