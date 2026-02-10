import json
import os

import anthropic
import pandas as pd

import config

VALIDATION_MODEL = "claude-opus-4-6"

VALIDATION_PROMPT = """\
You are a senior data analyst reviewing and CORRECTING a dataset of Ukrainian strikes on \
Russian territory (including Crimea) extracted from Telegram channels.

You will receive the dataset as CSV. Your job is to:

1. **FIX** any issues and return a corrected version of the full dataset as CSV.
2. Provide a brief report of what you changed.

Specific checks and fixes to apply:

**REMOVE rows that are:**
- NOT Ukrainian strikes on Russian territory (e.g. Russian strikes on Ukraine, frontline combat, generic "X drones shot down" with no target)
- Duplicates of another row (same incident reported twice — keep the more detailed one)
- Dates outside the expected range

**FIX fields:**
- Dates: must be YYYY-MM-DD format and within the expected range
- target_type: must accurately match the damage_summary description
- Maritime: set to True for tanker/vessel/platform attacks at sea, False for everything else
- Coordinates: if they clearly don't match the stated city/region, correct them or set to empty
- City/Region: standardize to English, fix obvious typos or inconsistencies
- Merge Source Channel lists if you identify duplicates being merged

**DO NOT add new rows.** Only correct existing data. The dataset must remain source-grounded.

**SORT** all rows chronologically by date.

Return your response in this exact format:

```csv
[the full corrected CSV here, with header row]
```

CHANGES:
- [bullet list of every change you made, referencing original row numbers]

QUALITY SCORE: [1-10]
"""


def validate(csv_path: str | None = None, api_key: str | None = None) -> str:
    """Validate and fix the final CSV using Claude Opus."""
    if not csv_path:
        csv_path = config.OUTPUT_CSV
    if not api_key:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not os.path.exists(csv_path):
        return "ERROR: CSV file not found at " + csv_path

    df = pd.read_csv(csv_path)
    print(f"Validating and fixing {len(df)} rows from {csv_path}...")

    csv_text = df.to_csv(index=False)

    client = anthropic.Anthropic(api_key=api_key)

    response = client.messages.create(
        model=VALIDATION_MODEL,
        max_tokens=16384,
        messages=[
            {
                "role": "user",
                "content": (
                    VALIDATION_PROMPT
                    + "\n\nDataset date range: "
                    + f"{config.START_DATE.strftime('%Y-%m-%d')} to "
                    + f"{config.END_DATE.strftime('%Y-%m-%d')}\n\n"
                    + f"CSV ({len(df)} rows):\n```\n{csv_text}\n```"
                ),
            }
        ],
    )

    result = response.content[0].text

    # Extract corrected CSV from response
    if "```csv" in result:
        csv_block = result.split("```csv")[1].split("```")[0].strip()
    elif "```" in result:
        csv_block = result.split("```")[1].split("```")[0].strip()
    else:
        csv_block = None

    if csv_block:
        # Parse and save corrected CSV
        from io import StringIO
        try:
            corrected_df = pd.read_csv(StringIO(csv_block))
            corrected_df.to_csv(csv_path, index=False, encoding="utf-8")
            print(f"  Corrected CSV saved: {len(df)} → {len(corrected_df)} rows")
        except Exception as e:
            print(f"  Warning: could not parse corrected CSV ({e}), keeping original")

    # Extract and save the changes report
    report_parts = result.split("CHANGES:")
    report = report_parts[1] if len(report_parts) > 1 else result

    report_path = os.path.join(config.DATA_DIR, "validation_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(result)
    print(f"  Validation report saved to {report_path}")

    return result


def run(api_key: str | None = None) -> str:
    """Run validation step."""
    print("\n" + "=" * 60)
    print("VALIDATION: Reviewing and fixing dataset with Claude Opus")
    print("=" * 60)
    return validate(api_key=api_key)
