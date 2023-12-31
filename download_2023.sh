#!/bin/bash

DIR="./data"

# Create the data directory if it doesn't exist
mkdir -p $DIR

# List of URLs to download
urls=(
"https://apps.irs.gov/pub/epostcard/990/xml/2023/index_2023.csv"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_01A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_02A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_03A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_04A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_05A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_05B.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_06A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_07A.zip"
"https://apps.irs.gov/pub/epostcard/990/xml/2023/2023_TEOS_XML_08A.zip"
)

# Loop through each URL and download it
for url in "${urls[@]}"; do
    # Use curl to download the file and save it to the ./data directory
    curl -L -o "./data/$(basename "$url")" "$url"
done

# Navigate to the source directory
cd "$DIR"

DEST_DIR="./xml"
mkdir -p "$DEST_DIR"
# Unzip each .zip file to the destination directory
for file in *.zip; do
    unzip "$file" -d "$DEST_DIR"  # Extract files into ./data/xml
done
