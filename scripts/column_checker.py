import pandas as pd

# Path to your raw GDELT file
file_path = "/Users/aniketdasurkar/Downloads/GDELT_CLEANED.tsv"

# Load the file as a DataFrame
df = pd.read_csv(file_path, sep="\t", dtype=str)

# Check for missing values in each column
# missing_counts = df.isna().sum() + df.eq("").sum()  # Treat empty strings as missing

# # Print columns with any missing values
# print("Columns with missing values:")
# print(missing_counts[missing_counts > 0])

# # Optional: show percentage of missing values per column
# percent_missing = 100 * missing_counts / len(df)
# print("\nPercentage of missing values per column:")
# print(percent_missing)

df_clean = df[df["Goldstein"].notna() & df["Goldstein"].str.strip().ne("")]

# Optional: reset index
df_clean.reset_index(drop=True, inplace=True)

# Save cleaned file
output_path = "GDELT.MASTERREDUCEDV2_CLEAN.TXT"
df_clean.to_csv(output_path, sep="\t", index=False)

print(f"Cleaned file saved to {output_path}")
print(f"Dropped {len(df) - len(df_clean)} rows with missing Goldstein values.")
