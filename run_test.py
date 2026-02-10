"""
Test runner: runs Steps 2-4 of the real pipeline on test_data/.
Only difference from main.py: config paths point at test_data/ instead of data/.
"""
import os
import sys

from dotenv import load_dotenv

load_dotenv()

# --- Patch config paths BEFORE importing pipeline modules ---
import config

config.DATA_DIR = "test_data"
config.RAW_DIR = f"{config.DATA_DIR}/raw"
config.EXTRACTED_DIR = f"{config.DATA_DIR}/extracted"
config.OUTPUT_CSV = f"{config.DATA_DIR}/ukraine_strikes_russia.csv"

# Now import pipeline modules (they read config.* at call time)
import filter_and_extract
import dedup
import validate


def main():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY must be set in .env")
        sys.exit(1)

    # Step 2: Filter + Extract (identical to main.py)
    print("=" * 60)
    print("STEP 2: Filtering and extracting incidents")
    print("=" * 60)
    incidents = filter_and_extract.run(api_key)

    # Step 3: Dedup + CSV (identical to main.py)
    print("\n" + "=" * 60)
    print("STEP 3: Deduplicating and exporting CSV")
    print("=" * 60)
    deduplicated = dedup.run(incidents)

    if deduplicated:
        dedup.to_csv(deduplicated)
        print(f"\nPipeline complete! Output: {config.OUTPUT_CSV}")
        print(f"Total unique incidents: {len(deduplicated)}")

        # Step 4: Validate with Opus (identical to main.py)
        print("\n")
        report = validate.run(api_key=api_key)
        try:
            print("\n" + report)
        except UnicodeEncodeError:
            # Windows console can't handle some Unicode chars in the report
            print("\n" + report.encode("ascii", errors="replace").decode("ascii"))
            print("  (Some characters replaced — full report in test_data/validation_report.md)")
    else:
        print("\nNo incidents found.")


if __name__ == "__main__":
    main()
