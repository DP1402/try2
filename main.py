import argparse
import os
import sys

from dotenv import load_dotenv

import config
import dedup
import filter_and_extract
import scrape
import validate


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Telegram Strike Scraper Pipeline")
    parser.add_argument("--skip-scrape", action="store_true", help="Skip scraping step")
    parser.add_argument("--skip-extract", action="store_true", help="Skip extraction step")
    parser.add_argument("--skip-validate", action="store_true", help="Skip Opus validation step")
    parser.add_argument("--only-scrape", action="store_true", help="Only run scraping")
    parser.add_argument("--only-extract", action="store_true", help="Only run extraction")
    parser.add_argument("--only-dedup", action="store_true", help="Only run dedup + CSV export")
    parser.add_argument("--only-validate", action="store_true", help="Only run Opus validation on existing CSV")
    args = parser.parse_args()

    api_id = os.getenv("TELEGRAM_API_ID")
    api_hash = os.getenv("TELEGRAM_API_HASH")
    anthropic_key = os.getenv("ANTHROPIC_API_KEY")

    # Validate credentials
    if not args.skip_scrape and not args.only_extract and not args.only_dedup and not args.only_validate:
        if not api_id or not api_hash:
            print("Error: TELEGRAM_API_ID and TELEGRAM_API_HASH must be set in .env")
            sys.exit(1)

    if not args.only_scrape:
        if not anthropic_key:
            print("Error: ANTHROPIC_API_KEY must be set in .env")
            sys.exit(1)

    # Only validate existing CSV
    if args.only_validate:
        report = validate.run(api_key=anthropic_key)
        try:
            print("\n" + report)
        except UnicodeEncodeError:
            print("\n" + report.encode("ascii", errors="replace").decode("ascii"))
            print("  (Some characters replaced — full report in data/validation_report.md)")
        return

    # Step 1: Scrape
    if not args.skip_scrape and not args.only_extract and not args.only_dedup:
        print("=" * 60)
        print("STEP 1: Scraping Telegram channels")
        print("=" * 60)
        scrape.run(int(api_id), api_hash)

    # Step 2: Filter + Extract
    incidents = None
    if not args.skip_extract and not args.only_scrape and not args.only_dedup:
        print("\n" + "=" * 60)
        print("STEP 2: Filtering and extracting incidents")
        print("=" * 60)
        incidents = filter_and_extract.run(anthropic_key)

    if args.only_scrape:
        print("\nScraping complete. Run with --skip-scrape to process.")
        return

    # Step 3: Dedup + CSV
    if not args.only_extract:
        print("\n" + "=" * 60)
        print("STEP 3: Deduplicating and exporting CSV")
        print("=" * 60)
        deduplicated = dedup.run(incidents)

        if deduplicated:
            dedup.to_csv(deduplicated)
            print(f"\nPipeline complete! Output: {config.OUTPUT_CSV}")
            print(f"Total unique incidents: {len(deduplicated)}")

            # Step 4: Validate with Opus
            if not args.skip_validate:
                report = validate.run(api_key=anthropic_key)
                try:
                    print("\n" + report)
                except UnicodeEncodeError:
                    print("\n" + report.encode("ascii", errors="replace").decode("ascii"))
                    print("  (Some characters replaced — full report in data/validation_report.md)")
        else:
            print("\nNo incidents found.")


if __name__ == "__main__":
    main()
