import pandas as pd


# Path to your raw GDELT file
file_path = "/Users/aniketdasurkar/Downloads/GDELT_CLEANED.tsv"

# Load as strings to avoid parsing issues
df = pd.read_csv(file_path, sep="\t", dtype=str)

print(f"Original rows: {len(df):,}")

# --- 1️⃣ Drop rows with missing or empty Goldstein ---
df = df[df["Goldstein"].notna() & df["Goldstein"].str.strip().ne("")]

print(f"After Goldstein filter: {len(df):,}")

# --- 2️⃣ Keep only events from year 2000 onwards ---
# Assumes column name is 'event_date' or similar
# If your column name is different, change it here
df = df[df["Date"].str.slice(0, 4).astype(int) >= 2013]

print(f"After year >= 2013 filter: {len(df):,}")

# Optional: reset index
df.reset_index(drop=True, inplace=True)

# --- 3️⃣ Save cleaned file ---
output_path = "GDELT.MASTERREDUCEDV2_CLEAN.TXT"
df.to_csv(output_path, sep="\t", index=False)

print(f"\nCleaned file saved to {output_path}")
