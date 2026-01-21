#python clean_gdelt_tsv.py ../../Downloads/GDELT.MASTERREDUCEDV2.TXT ../../Downloads/GDELT_CLEANED.tsv
import csv
import sys

INPUT_FILE = sys.argv[1]
OUTPUT_FILE = sys.argv[2]

# Columns we want to keep (must match header exactly)
COLUMNS_TO_KEEP = [
    "Date",
    "Source",
    "Target",
    "CAMEOCode",
    "QuadClass",
    "Goldstein",
    "NumEvents",
    "NumArts",
]

with open(INPUT_FILE, "r", encoding="utf-8", errors="ignore") as infile, \
     open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile, delimiter="\t")
    writer = csv.DictWriter(
        outfile,
        fieldnames=COLUMNS_TO_KEEP,
        delimiter="\t",
        extrasaction="ignore"
    )

    writer.writeheader()

    for row in reader:
        writer.writerow({col: row.get(col, "") for col in COLUMNS_TO_KEEP})

print(f"✅ Cleaned file written to: {OUTPUT_FILE}")
