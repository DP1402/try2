import json
import os
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import anthropic

import config
from dedup import _TRANSLIT

EXTRACTION_PROMPT = """\
You are extracting structured data about Ukrainian strikes on Russian-controlled territory.

SCOPE — Include:
- Strikes on Russian Federation territory (any region)
- Strikes on Russian-occupied Crimea
- Strikes on identifiable Russian military/infrastructure targets in other occupied areas
- Attacks on maritime targets (tankers, vessels, oil platforms) linked to Russia — tag these as maritime: true

SCOPE — Exclude:
- Russian strikes on Ukrainian cities — this is NOT what we want
- Generic "X drones shot down" Ministry of Defense summaries with no specific target hit or damage
- Frontline battlefield tactical actions (FPV drone combat, infantry clashes)
- Incidents with no identifiable target or location

CRITICAL RULES:
1. DATE: Extract the actual date the strike HAPPENED, not the date the message was posted. \
If the text says "on January 28" or "last night" (relative to message date), use that actual date. \
Only fall back to the message date if no specific date is mentioned or inferable. \
However, IGNORE dates from long retrospective summaries (e.g. "since the start of the war..." or "over the past year...").
2. MULTIPLE INCIDENTS: If one message describes strikes on 3 different targets, return 3 separate objects.
3. LANGUAGE: All output fields (city, region, facility_name, damage_summary) must be in English.
4. COORDINATES: Only provide if you are reasonably sure of the location. Use null otherwise.

For each incident extract:
- date: the actual date the strike happened (YYYY-MM-DD). Use context clues from the text \
("last night", "on January 28", "yesterday"). Use message date only as fallback.
- city: city or settlement name in English
- region: region/oblast name in English
- target_type: one of [military_base, airfield, ammunition_depot, fuel_depot, oil_refinery, power_infrastructure, naval, radar, command_post, transport, industrial, residential, other]
- facility_name: specific facility name in English transliteration, or null
- damage_summary: concise English description of what was hit and what happened
- latitude: float or null
- longitude: float or null
- confidence: high (confirmed strike with details) / medium (likely strike, some details) / low (unconfirmed, vague)
- maritime: true if this is an attack on a maritime target (tanker, vessel, oil platform at sea), false otherwise

Return a JSON array matching the input message order.
- For a message with no relevant incidents: null
- For a message with 1 incident: one object
- For a message with N incidents: N objects in a nested array

Return ONLY valid JSON, no other text.\
"""


def _has_any(text: str, keywords: list[str]) -> bool:
    """Check if text contains any keyword (substring match)."""
    return any(kw in text for kw in keywords)


def _compound_keyword_filter(text: str) -> bool:
    """
    Compound pre-filter: require an action term AND at least one of
    (Russian location, infrastructure term, or damage term).
    This eliminates ~80% of irrelevant messages vs. single-keyword matching.
    """
    text_lower = text.lower()

    # Must have an action/strike term
    if not _has_any(text_lower, config.KEYWORDS_ACTION):
        return False

    # Must also have at least one of: location, infrastructure, or damage
    has_location = _has_any(text_lower, config.KEYWORDS_RUSSIAN_LOCATIONS)
    has_infra = _has_any(text_lower, config.KEYWORDS_INFRASTRUCTURE)
    has_damage = _has_any(text_lower, config.KEYWORDS_DAMAGE)

    if not (has_location or has_infra or has_damage):
        return False

    # Exclusion: if message mentions Ukrainian cities being hit
    # but NO Russian locations, it's probably about Russian strikes on Ukraine
    has_ua_target = _has_any(text_lower, config.KEYWORDS_UKRAINIAN_TARGETS)
    if has_ua_target and not has_location:
        return False

    return True


def _word_set(text: str) -> set[str]:
    """Extract word set for similarity comparison (transliterated to Latin)."""
    return set(text.lower().translate(_TRANSLIT).split())


def _jaccard_similarity(a: set, b: set) -> float:
    """Jaccard similarity between two sets."""
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _cross_channel_dedup(messages: list[dict]) -> list[dict]:
    """
    Remove near-duplicate messages across channels for the same day.
    Keeps the longest version and merges source channels.
    """
    # Group by date (day)
    by_date: dict[str, list[dict]] = defaultdict(list)
    for msg in messages:
        day = msg["date"][:10]
        by_date[day].append(msg)

    result = []
    for day, day_msgs in by_date.items():
        # Compare all pairs within the same day
        used = [False] * len(day_msgs)
        clusters: list[list[int]] = []

        for i in range(len(day_msgs)):
            if used[i]:
                continue
            cluster = [i]
            used[i] = True
            words_i = _word_set(day_msgs[i]["text"])

            for j in range(i + 1, len(day_msgs)):
                if used[j]:
                    continue
                # Skip same-channel pairs (those are distinct messages)
                if day_msgs[i]["channel"] == day_msgs[j]["channel"]:
                    continue
                words_j = _word_set(day_msgs[j]["text"])
                if _jaccard_similarity(words_i, words_j) >= config.DEDUP_SIMILARITY:
                    cluster.append(j)
                    used[j] = True
            clusters.append(cluster)

        for cluster in clusters:
            # Keep the longest message, merge channels
            cluster_msgs = [day_msgs[i] for i in cluster]
            cluster_msgs.sort(key=lambda m: len(m["text"]), reverse=True)
            best = dict(cluster_msgs[0])
            channels = set(m["channel"] for m in cluster_msgs)
            best["_source_channels"] = list(channels)
            result.append(best)

    return result


def load_and_filter_messages() -> list[dict]:
    """Load raw JSONL files, apply compound keyword filter."""
    messages = []
    total_loaded = 0

    for filename in os.listdir(config.RAW_DIR):
        if not filename.endswith(".jsonl"):
            continue
        filepath = os.path.join(config.RAW_DIR, filename)
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                total_loaded += 1
                text = msg.get("text", "")

                # Compound keyword match
                if not _compound_keyword_filter(text):
                    continue

                messages.append(msg)

    print(f"  Loaded {total_loaded} raw messages")
    print(f"  {len(messages)} passed compound keyword filter "
          f"({100 - len(messages) / max(total_loaded, 1) * 100:.0f}% eliminated)")

    # Layer 3: cross-channel dedup
    before_dedup = len(messages)
    messages = _cross_channel_dedup(messages)
    print(f"  {len(messages)} after cross-channel dedup "
          f"({before_dedup - len(messages)} near-duplicates removed)")

    return messages


def _build_batch_prompt(batch: list[dict]) -> str:
    """Build the user prompt for a batch of messages."""
    lines = []
    for i, msg in enumerate(batch):
        channels = msg.get("_source_channels", [msg["channel"]])
        ch_str = ", ".join(f"@{c}" for c in channels)
        lines.append(
            f"[Message {i + 1}] Channels: {ch_str} | "
            f"Date: {msg['date'][:10]}\n{msg['text']}"
        )
    return "\n\n---\n\n".join(lines)


def _send_batch(client: anthropic.Anthropic, batch: list[dict], batch_idx: int,
                total_batches: int) -> list[dict]:
    """Send a single batch to Claude and return extracted incidents."""
    user_prompt = _build_batch_prompt(batch)
    incidents = []

    for attempt in range(config.MAX_RETRIES):
        try:
            response = client.messages.create(
                model=config.CLAUDE_MODEL,
                max_tokens=4096,
                messages=[
                    {"role": "user", "content": EXTRACTION_PROMPT + "\n\n" + user_prompt}
                ],
            )
            break
        except anthropic.RateLimitError:
            wait = config.RETRY_DELAY * (2 ** attempt)
            print(f"    Batch {batch_idx + 1}: rate limited, waiting {wait}s...")
            time.sleep(wait)
        except anthropic.APIError as e:
            if attempt < config.MAX_RETRIES - 1:
                wait = config.RETRY_DELAY * (2 ** attempt)
                print(f"    Batch {batch_idx + 1}: API error ({e}), retry in {wait}s...")
                time.sleep(wait)
            else:
                print(f"    Batch {batch_idx + 1}: failed after {config.MAX_RETRIES} attempts, skipping.")
                return []
    else:
        return []

    # Parse response
    text = response.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

    try:
        results = json.loads(text)
    except json.JSONDecodeError:
        print(f"    Batch {batch_idx + 1}: could not parse response, skipping.")
        return []

    # Process results — handle nested arrays (multiple incidents per message)
    for i, result in enumerate(results):
        if result is None or i >= len(batch):
            continue

        msg = batch[i]
        channels = msg.get("_source_channels", [msg["channel"]])

        # result can be a single dict or a list of dicts (multiple incidents)
        entries = result if isinstance(result, list) else [result]

        for entry in entries:
            if entry is None:
                continue
            entry["source_channel"] = ", ".join(sorted(channels))
            entry["source_message_id"] = str(msg["message_id"])
            entry["original_text"] = msg["text"]
            # Store message timestamp separately from event date
            msg_date = msg["date"][:10]
            entry["message_date"] = msg_date
            # Fall back to message date if Claude returned no event date
            if not entry.get("date") or not entry["date"].startswith("202"):
                entry["date"] = msg_date
            incidents.append(entry)

    print(f"    Batch {batch_idx + 1}/{total_batches}: {len(incidents)} incidents", flush=True)
    return incidents


def extract_incidents(api_key: str, messages: list[dict], auto_confirm: bool = False) -> list[dict]:
    """Send messages to Claude in parallel batches and extract incidents."""
    client = anthropic.Anthropic(api_key=api_key)
    os.makedirs(config.EXTRACTED_DIR, exist_ok=True)

    batches = []
    for i in range(0, len(messages), config.BATCH_SIZE):
        batches.append(messages[i:i + config.BATCH_SIZE])

    total_batches = len(batches)

    # --- Cost estimate ---
    # Count actual characters across all batches, convert to tokens
    total_chars = sum(len(msg.get("text", "")) for msg in messages)
    # ~3 chars per token for Cyrillic/mixed text
    input_tokens_content = total_chars / 3
    # System prompt overhead per batch (~800 tokens each)
    input_tokens_system = total_batches * 800
    total_input_tokens = int(input_tokens_content + input_tokens_system)
    # Output estimate: ~200 tokens per message × 30% incident rate
    total_output_tokens = int(len(messages) * 200 * 0.3)
    # Sonnet pricing: $3/M input + $15/M output
    est_input_cost = total_input_tokens / 1_000_000 * 3
    est_output_cost = total_output_tokens / 1_000_000 * 15
    est_total_cost = est_input_cost + est_output_cost

    print(f"\n  --- Extraction Cost Estimate (Sonnet 4.5) ---", flush=True)
    print(f"  Messages: {len(messages)} in {total_batches} batches", flush=True)
    print(f"  Input tokens:  ~{total_input_tokens:,} (${est_input_cost:.2f})", flush=True)
    print(f"  Output tokens: ~{total_output_tokens:,} (${est_output_cost:.2f})", flush=True)
    print(f"  Estimated total cost: ${est_total_cost:.2f}", flush=True)
    print(flush=True)

    if auto_confirm:
        print("  Proceed with extraction? [y/N]: y (auto-confirmed)")
    else:
        confirm = input("  Proceed with extraction? [y/N]: ").strip().lower()
        if confirm != "y":
            print("  Extraction cancelled.")
            return []

    all_incidents = []
    output_path = os.path.join(config.EXTRACTED_DIR, "incidents.jsonl")
    file_lock = threading.Lock()
    completed_count = [0]  # mutable container for thread-safe counter

    # Open file for incremental writing — each batch's results saved immediately
    with open(output_path, "w", encoding="utf-8") as outfile:
        with ThreadPoolExecutor(max_workers=config.MAX_CONCURRENT) as executor:
            futures = {
                executor.submit(_send_batch, client, batch, idx, total_batches): idx
                for idx, batch in enumerate(batches)
            }
            for future in as_completed(futures):
                try:
                    incidents = future.result()
                    with file_lock:
                        for inc in incidents:
                            outfile.write(json.dumps(inc, ensure_ascii=False) + "\n")
                        outfile.flush()
                        all_incidents.extend(incidents)
                        completed_count[0] += 1
                    print(f"    [{completed_count[0]}/{total_batches} batches done, "
                          f"{len(all_incidents)} incidents so far]", flush=True)
                except Exception as e:
                    print(f"    Batch error: {e}", flush=True)

    print(f"  Extracted {len(all_incidents)} incidents total.", flush=True)
    return all_incidents


def run(api_key: str, auto_confirm: bool = False) -> list[dict]:
    """Full filter + extract pipeline."""
    print("Loading and filtering messages...")
    messages = load_and_filter_messages()

    if not messages:
        print("  No messages to process.")
        return []

    print(f"\nExtracting incidents with Claude API ({config.CLAUDE_MODEL})...")
    return extract_incidents(api_key, messages, auto_confirm=auto_confirm)
