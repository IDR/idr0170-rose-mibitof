#!/bin/bash

# Script to process each line of a TSV file
# Takes two columns from each line and passes them as arguments to a command
# Usage: ./process_tsv.sh <path_to_tsv_file>

# Check if TSV file path is provided as argument
if [[ $# -eq 0 ]]; then
    echo "Usage: $0 <path_to_tsv_file>"
    echo "Example: $0 ../experimentA/idr0170-experimentA-filePaths.tsv"
    exit 1
fi

# Set the path to the TSV file from command line argument
TSV_FILE="$1"

# Check if TSV file exists
if [[ ! -f "$TSV_FILE" ]]; then
    echo "Error: TSV file not found at $TSV_FILE"
    exit 1
fi

# Function to process each line
register() {
    local dataset_name="$1"
    local url="$2"
    local image_name="$3"

    echo "Processing: Dataset=$dataset_name, URL=$url, Image=$image_name"
    
    python ~/biongff/omero-import-utils/metadata/register.py --nosignrequest --name "$image_name" --target-by-name "$dataset_name" "$url"
}

# Main processing loop
echo "Starting to process TSV file: $TSV_FILE"
echo "Found $(wc -l < "$TSV_FILE") lines to process"
echo "----------------------------------------"

line_count=0
while IFS=$'\t' read -r dataset_name url image_name; do
    # Skip empty lines
    if [[ -z "$dataset_name" || -z "$url" || -z "$image_name" ]]; then
        continue
    fi
    
    line_count=$((line_count + 1))
    echo "Line $line_count:"
    
    # Process the line
    register "$dataset_name" "$url" "$image_name"
    
    echo "----------------------------------------"
done < "$TSV_FILE"

echo "Completed processing $line_count lines"
