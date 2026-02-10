#!/usr/bin/env python3
"""
Test harness: creates synthetic Telegram messages for one week,
runs the keyword filter + cross-channel dedup + incident dedup
to surface false positives, false negatives, and merging bugs.

No Telegram or Claude API calls — everything is local.
"""

import json
import os
import sys
import tempfile
from datetime import datetime, timedelta

# Patch config paths to use a temp directory
import config

TMPDIR = tempfile.mkdtemp(prefix="strikes_test_")
config.RAW_DIR = os.path.join(TMPDIR, "raw")
config.EXTRACTED_DIR = os.path.join(TMPDIR, "extracted")
config.OUTPUT_CSV = os.path.join(TMPDIR, "test_output.csv")
os.makedirs(config.RAW_DIR)
os.makedirs(config.EXTRACTED_DIR)

import filter_and_extract
import dedup

# ──────────────────────────────────────────────────────────────
# Section 1: Synthetic scraped messages (raw JSONL)
# ──────────────────────────────────────────────────────────────

BASE_DATE = datetime(2026, 2, 3)

def make_msg(channel, msg_id, text, day_offset=0, hour=12):
    dt = BASE_DATE + timedelta(days=day_offset, hours=hour)
    return {
        "message_id": msg_id,
        "date": dt.isoformat(),
        "text": text,
        "channel": channel,
    }


MESSAGES = [
    # ── TRUE POSITIVES: should pass filter ──

    # TP1: Classic oil refinery strike in Krasnodar
    make_msg("astrapress", 1001,
             "Ночью дроны ВСУ атаковали НПЗ в Краснодарском крае. Сообщается о сильном пожаре на территории нефтеперерабатывающего завода.",
             day_offset=0),

    # TP2: Same event reported on a different channel (cross-channel duplicate)
    make_msg("oper_ZSU", 2001,
             "Ночью украинские дроны атаковали НПЗ в Краснодарском крае, мощный пожар на нефтеперерабатывающем заводе.",
             day_offset=0),

    # TP3: Crimea military base strike
    make_msg("Crimeanwind", 3001,
             "Удар по военной базе в Крыму. Сообщается о серии взрывов в районе Джанкоя.",
             day_offset=1),

    # TP4: Belgorod fuel depot
    make_msg("Tsaplienko", 4001,
             "ВСУ нанесли удар по топливному складу в Белгородской области. Горит нефтебаза.",
             day_offset=2),

    # TP5: Maritime target — tanker in Black Sea
    make_msg("supernova_plus", 5001,
             "В Черном море атакован российский танкер. Удар морскими дронами.",
             day_offset=3),

    # TP6: Ukrainian-language post about Bryansk ammo depot
    make_msg("exilenova_plus", 6001,
             "Вибухи на складі боєприпасів у Брянській області. Удар дронами ВСУ вночі.",
             day_offset=4),

    # TP7: English-language post (some channels post in English)
    make_msg("astrapress", 7001,
             "Ukrainian drone strike hit an oil refinery in Saratov region. Massive fire reported.",
             day_offset=5),

    # TP8: Power infrastructure
    make_msg("oper_ZSU", 8001,
             "Атака дронов ВСУ на энергообъект в Курской области. Подстанция повреждена.",
             day_offset=5),

    # ── FALSE POSITIVES: should be REJECTED but may sneak through ──

    # FP1: Russian strike on Ukraine (mentions Belgorod only as launch point)
    make_msg("Tsaplienko", 9001,
             "Россия нанесла ракетный удар по Харькову. Ракеты запущены из Белгородской области. Повреждены жилые дома.",
             day_offset=0),

    # FP2: Mentions "Rostov" but this is about a car accident with "explosion"
    make_msg("astrapress", 9002,
             "В Ростове произошел сильный взрыв газа в жилом доме. Пострадали 5 человек. Атака не подтверждена, взрыв бытового газа.",
             day_offset=1),

    # FP3: Generic MoD summary "X drones shot down" with no specific target
    make_msg("oper_ZSU", 9003,
             "По данным ПВО, за ночь сбито 15 дронов-шахедов над Ростовской областью. Все дроны уничтожены.",
             day_offset=2),

    # FP4: Historical/retrospective summary
    make_msg("supernova_plus", 9004,
             "С начала 2025 года ВСУ нанесли более 200 ударов по нефтеперерабатывающим заводам России. "
             "НПЗ в Рязани, Саратове, Самаре — все были атакованы.",
             day_offset=3),

    # FP5: Frontline tactical combat (mentions "military base" but is frontline action)
    make_msg("exilenova_plus", 9005,
             "На Курском направлении ВСУ атаковали позиции врага FPV-дронами. "
             "Уничтожена военная база противника на передовой. Бои продолжаются.",
             day_offset=4),

    # FP6: Short stem false positive — "рф" inside a longer word
    make_msg("Crimeanwind", 9006,
             "Информация о тарифных изменениях в сфере энергетики. Удар по кошельку потребителей.",
             day_offset=5),

    # FP7: "база" means "database" in IT context
    make_msg("astrapress", 9007,
             "Хакерская атака на базу данных российского министерства. Пожар в серверной.",
             day_offset=0),

    # ── FALSE NEGATIVES: should PASS but might get filtered out ──

    # FN1: Strike report without explicit action word (uses "поражение" stem that isn't in keywords)
    make_msg("Tsaplienko", 10001,
             "Поражение нефтеперерабатывающего завода в Ростовской области. Мощный пожар.",
             day_offset=1),

    # FN2: Uses uncommon weapon name not in keywords
    make_msg("oper_ZSU", 10002,
             "БОБР ВСУ поразил электростанцию в Белгородской области. Крупный пожар.",
             day_offset=2),

    # FN3: Short message, only city name hint (no explicit infrastructure term)
    make_msg("Crimeanwind", 10003,
             "Удар по Крыму. Детали уточняются.",
             day_offset=3),

    # FN4: Aftermath report with no action keyword, just damage
    make_msg("supernova_plus", 10004,
             "Пожар на нефтебазе в Краснодарском крае продолжается уже вторые сутки.",
             day_offset=4),

    # FN5: Ukrainian language, refers to Crimea bridge attack
    make_msg("exilenova_plus", 10005,
             "Атака на Кримський міст. Рух зупинено.",
             day_offset=5),

    # ── EDGE CASES for dedup ──

    # EC1 & EC2: Two DIFFERENT targets in same city, same day — should NOT be merged
    make_msg("astrapress", 11001,
             "Удар по НПЗ в Рязани. Пожар на нефтеперерабатывающем заводе.",
             day_offset=6),
    make_msg("oper_ZSU", 11002,
             "Удар по военному аэродрому в Рязани. Повреждены взлётные полосы.",
             day_offset=6),

    # EC3 & EC4: Same target reported 3 days apart — borderline for date window
    make_msg("Tsaplienko", 12001,
             "Повторный удар дронами по НПЗ Ильского в Краснодарском крае.",
             day_offset=0),
    make_msg("Crimeanwind", 12002,
             "Третий удар за неделю по Ильскому НПЗ в Краснодарском крае. Новый пожар.",
             day_offset=3),
]

# ──────────────────────────────────────────────────────────────
# Section 2: Write messages to JSONL files grouped by channel
# ──────────────────────────────────────────────────────────────

from collections import defaultdict

by_channel = defaultdict(list)
for msg in MESSAGES:
    by_channel[msg["channel"]].append(msg)

for ch, msgs in by_channel.items():
    path = os.path.join(config.RAW_DIR, f"{ch}.jsonl")
    with open(path, "w", encoding="utf-8") as f:
        for m in msgs:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")

print(f"Wrote {len(MESSAGES)} synthetic messages across {len(by_channel)} channels")
print(f"Test data dir: {TMPDIR}")
print()

# ──────────────────────────────────────────────────────────────
# Section 3: Run keyword filter
# ──────────────────────────────────────────────────────────────

print("=" * 70)
print("KEYWORD FILTER TEST")
print("=" * 70)

# Classify each message
tp_ids = {1001, 2001, 3001, 4001, 5001, 6001, 7001, 8001}
fp_ids = {9001, 9002, 9003, 9004, 9005, 9006, 9007}
fn_ids = {10001, 10002, 10003, 10004, 10005}
ec_ids = {11001, 11002, 12001, 12002}

filter_results = {"tp_pass": [], "tp_fail": [], "fp_pass": [], "fp_reject": [],
                  "fn_pass": [], "fn_fail": [], "ec_pass": [], "ec_fail": []}

for msg in MESSAGES:
    mid = msg["message_id"]
    passed = filter_and_extract._compound_keyword_filter(msg["text"])

    if mid in tp_ids:
        if passed:
            filter_results["tp_pass"].append(mid)
        else:
            filter_results["tp_fail"].append(mid)
    elif mid in fp_ids:
        if passed:
            filter_results["fp_pass"].append(mid)
        else:
            filter_results["fp_reject"].append(mid)
    elif mid in fn_ids:
        if passed:
            filter_results["fn_pass"].append(mid)
        else:
            filter_results["fn_fail"].append(mid)
    elif mid in ec_ids:
        if passed:
            filter_results["ec_pass"].append(mid)
        else:
            filter_results["ec_fail"].append(mid)

print(f"\nTrue Positives:  {len(filter_results['tp_pass'])}/{len(tp_ids)} passed (should be all)")
for mid in filter_results['tp_fail']:
    msg = next(m for m in MESSAGES if m["message_id"] == mid)
    print(f"  MISSED TP {mid}: {msg['text'][:80]}...")

print(f"\nFalse Positives: {len(filter_results['fp_pass'])}/{len(fp_ids)} leaked through (should be 0)")
for mid in filter_results['fp_pass']:
    msg = next(m for m in MESSAGES if m["message_id"] == mid)
    print(f"  LEAKED FP {mid}: {msg['text'][:80]}...")

print(f"\nFalse Negatives: {len(filter_results['fn_fail'])}/{len(fn_ids)} missed (filter too strict)")
for mid in filter_results['fn_fail']:
    msg = next(m for m in MESSAGES if m["message_id"] == mid)
    print(f"  MISSED FN {mid}: {msg['text'][:80]}...")

print(f"\nEdge Cases:      {len(filter_results['ec_pass'])}/{len(ec_ids)} passed")

# ──────────────────────────────────────────────────────────────
# Section 4: Run cross-channel dedup on filtered messages
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("CROSS-CHANNEL DEDUP TEST")
print("=" * 70)

filtered = [m for m in MESSAGES if filter_and_extract._compound_keyword_filter(m["text"])]
print(f"\n{len(filtered)} messages passed filter")

deduped = filter_and_extract._cross_channel_dedup(filtered)
print(f"{len(deduped)} after cross-channel dedup ({len(filtered) - len(deduped)} removed)")

# Check: TP1 and TP2 should be merged
tp1_2_merged = False
for msg in deduped:
    channels = msg.get("_source_channels", [msg["channel"]])
    if "astrapress" in channels and "oper_ZSU" in channels:
        tp1_2_merged = True
        break
print(f"\nTP1+TP2 (same event, diff channels) merged: {tp1_2_merged}")

# Check: EC1 and EC2 (different targets, same city, same day) should NOT be merged
ec1_present = any(m["message_id"] == 11001 for m in deduped)
ec2_present = any(m["message_id"] == 11002 for m in deduped)
print(f"EC1 (refinery) still present: {ec1_present}")
print(f"EC2 (airfield) still present: {ec2_present}")
if not (ec1_present and ec2_present):
    print("  BUG: Different targets in same city were incorrectly merged!")

# ──────────────────────────────────────────────────────────────
# Section 5: Run incident-level dedup on synthetic extraction results
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("INCIDENT DEDUP TEST")
print("=" * 70)

# Simulate what Claude would extract
synthetic_incidents = [
    # Pair A: Same refinery hit, 2 sources — should merge
    {"date": "2026-02-03", "city": "Krasnodar", "region": "Krasnodar",
     "target_type": "oil_refinery", "facility_name": "Krasnodar Oil Refinery",
     "damage_summary": "Oil refinery hit by drones, fire reported",
     "latitude": 45.04, "longitude": 38.97, "confidence": "high", "maritime": False,
     "source_channel": "astrapress", "message_date": "2026-02-03"},
    {"date": "2026-02-03", "city": "Krasnodar", "region": "Krasnodar",
     "target_type": "oil_refinery", "facility_name": None,
     "damage_summary": "Drone strike on refinery",
     "latitude": 45.04, "longitude": 38.97, "confidence": "medium", "maritime": False,
     "source_channel": "oper_ZSU", "message_date": "2026-02-03"},

    # Pair B: DIFFERENT targets in Ryazan — should NOT merge
    {"date": "2026-02-09", "city": "Ryazan", "region": "Ryazan",
     "target_type": "oil_refinery", "facility_name": "Ryazan Oil Refinery",
     "damage_summary": "Oil refinery struck, large fire",
     "latitude": 54.62, "longitude": 39.70, "confidence": "high", "maritime": False,
     "source_channel": "astrapress", "message_date": "2026-02-09"},
    {"date": "2026-02-09", "city": "Ryazan", "region": "Ryazan",
     "target_type": "airfield", "facility_name": "Dyagilevo airfield",
     "damage_summary": "Military airfield struck, runway damaged",
     "latitude": 54.63, "longitude": 39.57, "confidence": "high", "maritime": False,
     "source_channel": "oper_ZSU", "message_date": "2026-02-09"},

    # Pair C: Same city, same target type, coordinates ~15km apart — should they merge?
    {"date": "2026-02-05", "city": "Bryansk", "region": "Bryansk",
     "target_type": "ammunition_depot", "facility_name": None,
     "damage_summary": "Ammo depot hit",
     "latitude": 53.25, "longitude": 34.37, "confidence": "high", "maritime": False,
     "source_channel": "exilenova_plus", "message_date": "2026-02-05"},
    {"date": "2026-02-05", "city": "Bryansk", "region": "Bryansk",
     "target_type": "ammunition_depot", "facility_name": "Bryansk arsenal",
     "damage_summary": "Explosions at ammunition storage facility near Bryansk",
     "latitude": 53.35, "longitude": 34.40, "confidence": "medium", "maritime": False,
     "source_channel": "Tsaplienko", "message_date": "2026-02-05"},

    # Pair D: 50km radius problem — two cities in same oblast, 45km apart
    {"date": "2026-02-06", "city": "Belgorod", "region": "Belgorod",
     "target_type": "fuel_depot", "facility_name": "Belgorod fuel depot",
     "damage_summary": "Fuel depot struck",
     "latitude": 50.60, "longitude": 36.58, "confidence": "high", "maritime": False,
     "source_channel": "Tsaplienko", "message_date": "2026-02-06"},
    {"date": "2026-02-06", "city": "Stary Oskol", "region": "Belgorod",
     "target_type": "fuel_depot", "facility_name": "Stary Oskol fuel storage",
     "damage_summary": "Fuel storage facility hit",
     "latitude": 51.30, "longitude": 37.84, "confidence": "high", "maritime": False,
     "source_channel": "astrapress", "message_date": "2026-02-06"},

    # Pair E: "other" target type matches anything — merging a radar with "other"
    {"date": "2026-02-07", "city": "Sevastopol", "region": "Crimea",
     "target_type": "radar", "facility_name": "S-400 radar station",
     "damage_summary": "Radar installation destroyed",
     "latitude": 44.60, "longitude": 33.52, "confidence": "high", "maritime": False,
     "source_channel": "Crimeanwind", "message_date": "2026-02-07"},
    {"date": "2026-02-07", "city": "Sevastopol", "region": "Crimea",
     "target_type": "other", "facility_name": None,
     "damage_summary": "Military target in Sevastopol hit",
     "latitude": 44.58, "longitude": 33.53, "confidence": "medium", "maritime": False,
     "source_channel": "oper_ZSU", "message_date": "2026-02-07"},

    # Pair F: first-5-chars city name collision — "Krasn..." matches both cities
    {"date": "2026-02-04", "city": "Krasnodar", "region": "Krasnodar",
     "target_type": "fuel_depot", "facility_name": None,
     "damage_summary": "Fuel depot hit in Krasnodar",
     "latitude": None, "longitude": None, "confidence": "medium", "maritime": False,
     "source_channel": "astrapress", "message_date": "2026-02-04"},
    {"date": "2026-02-04", "city": "Krasnoyarsk", "region": "Krasnoyarsk",
     "target_type": "fuel_depot", "facility_name": None,
     "damage_summary": "Fuel depot hit in Krasnoyarsk",
     "latitude": None, "longitude": None, "confidence": "medium", "maritime": False,
     "source_channel": "Tsaplienko", "message_date": "2026-02-04"},
]

print(f"\n{len(synthetic_incidents)} synthetic incidents")

result = dedup.deduplicate(synthetic_incidents)
print(f"{len(result)} after dedup")

# Analyze which pairs got merged
print("\nExpected behavior vs actual:")

# Pair A: should merge (same event)
pair_a_merged = len([r for r in result
    if r.get("city") == "Krasnodar" and r.get("date") == "2026-02-03"
    and r.get("target_type") == "oil_refinery"]) == 1
print(f"  Pair A (same refinery, 2 sources): merged={pair_a_merged} (expected: True)")

# Pair B: should NOT merge (different targets)
pair_b_count = len([r for r in result
    if r.get("city") == "Ryazan" and r.get("date") == "2026-02-09"])
print(f"  Pair B (refinery vs airfield, same city): count={pair_b_count} (expected: 2)")

# Pair C: same city, same type, 15km apart — likely merged
pair_c_count = len([r for r in result
    if r.get("city") == "Bryansk" and r.get("date") == "2026-02-05"])
print(f"  Pair C (2 ammo depots, 15km apart): count={pair_c_count} (expected: 2, but may be 1)")

# Pair D: different cities, 80km apart
pair_d_count = len([r for r in result
    if r.get("region") == "Belgorod" and r.get("date") == "2026-02-06"])
print(f"  Pair D (Belgorod vs Stary Oskol, ~80km): count={pair_d_count} (expected: 2)")

# Pair E: "other" matches radar
pair_e_count = len([r for r in result
    if r.get("city") == "Sevastopol" and r.get("date") == "2026-02-07"])
print(f"  Pair E (radar + 'other' in Sevastopol): count={pair_e_count} (expected: 2, got merged?)")

# Pair F: first-5-chars collision
pair_f_count = len([r for r in result
    if r.get("date") == "2026-02-04" and r.get("target_type") == "fuel_depot"])
print(f"  Pair F (Krasnodar vs Krasnoyarsk, first-5-chars): count={pair_f_count} (expected: 2)")

# ──────────────────────────────────────────────────────────────
# Section 6: Test the transliteration / normalization edge cases
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("NORMALIZATION TEST")
print("=" * 70)

test_pairs = [
    ("Белгород", "Belgorod"),
    ("Краснодар", "Krasnodar"),
    ("Рязань", "Ryazan"),
    ("Севастополь", "Sevastopol"),
    ("Брянськ", "Bryansk"),    # Ukrainian spelling
    ("Воронiж", "Voronezh"),   # Ukrainian spelling
]

for cyrillic, english in test_pairs:
    norm_c = dedup._normalize(cyrillic)
    norm_e = dedup._normalize(english)
    match = norm_c == norm_e
    print(f"  '{cyrillic}' → '{norm_c}' vs '{english}' → '{norm_e}'  match={match}")

# ──────────────────────────────────────────────────────────────
# Section 7: Check extraction prompt token budget
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("TOKEN BUDGET CHECK")
print("=" * 70)

# Estimate: extraction prompt is ~500 tokens, each message ~200 tokens
# With 25 messages per batch: ~500 + 25*200 = ~5500 input tokens
# max_tokens=4096 for output

prompt_chars = len(filter_and_extract.EXTRACTION_PROMPT)
print(f"Extraction prompt: {prompt_chars} chars (~{prompt_chars // 4} tokens)")
print(f"Batch size: {config.BATCH_SIZE} messages")
print(f"Estimated input per batch: ~{prompt_chars // 4 + config.BATCH_SIZE * 200} tokens")
print(f"Output max_tokens: 4096")
print(f"At ~150 tokens per incident, max ~{4096 // 150} incidents per batch")
print(f"If all 25 messages have 2+ incidents each = 50+ incidents = ~7500 tokens — WILL TRUNCATE")

# ──────────────────────────────────────────────────────────────
# Section 8: Validate.py CSV round-trip issue
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("CSV ROUND-TRIP CHECK (validate.py sends index=True)")
print("=" * 70)

import pandas as pd
from io import StringIO

sample_df = pd.DataFrame([
    {"Date": "2026-02-03", "City": "Krasnodar", "Region": "Krasnodar",
     "Target Type": "oil_refinery", "Damage Summary": "Fire at refinery"}
])

csv_with_index = sample_df.to_csv(index=True)
print(f"CSV sent to Claude Opus (index=True):")
print(csv_with_index)
print("  ^ Note the unnamed index column — Claude may return it as data,")
print("    or may confuse row numbers with data fields.")

csv_without = sample_df.to_csv(index=False)
print(f"Should be (index=False):")
print(csv_without)

# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)

# Check for .env existence
env_exists = os.path.exists("/home/user/try2/.env")
print(f"\n.env file present: {env_exists}")
if not env_exists:
    print("  (No API keys available — scraping and extraction cannot run)")
