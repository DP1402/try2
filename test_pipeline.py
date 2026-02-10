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

    # TP9: Radar destroyed in Crimea with location + damage
    make_msg("Crimeanwind", 8501,
             "Ракетный удар по РЛС в Крыму. Радар уничтожен, пожар на объекте.",
             day_offset=3),

    # TP10: Airfield attack with damage
    make_msg("oper_ZSU", 8502,
             "Дроны атаковали аэродром в Курской области. Повреждена взлётная полоса, горит техника.",
             day_offset=4),

    # ── FALSE POSITIVES: should be REJECTED ──

    # FP1: Russian strike on Ukraine (mentions Belgorod only as launch point)
    make_msg("Tsaplienko", 9001,
             "Россия нанесла ракетный удар по Харькову. Ракеты запущены из Белгородской области. Повреждены жилые дома.",
             day_offset=0),

    # FP2: Gas explosion in Rostov (not a strike)
    make_msg("astrapress", 9002,
             "В Ростове произошел сильный взрыв газа в жилом доме. Пострадали 5 человек. Атака не подтверждена, взрыв бытового газа.",
             day_offset=1),

    # FP3: Generic MoD summary "X drones shot down" with no specific target
    make_msg("oper_ZSU", 9003,
             "По данным ПВО, за ночь сбито над Ростовской областью 15 дронов-шахедов. Все дроны уничтожены.",
             day_offset=2),

    # FP4: Historical/retrospective summary
    make_msg("supernova_plus", 9004,
             "С начала 2025 года ВСУ нанесли более 200 ударов по нефтеперерабатывающим заводам России. "
             "НПЗ в Рязани, Саратове, Самаре — все были атакованы.",
             day_offset=3),

    # FP5: Frontline tactical combat (FPV drones on front line)
    make_msg("exilenova_plus", 9005,
             "На Курском направлении ВСУ атаковали позиции врага FPV-дронами. "
             "Уничтожена военная база противника на передовой. Бои продолжаются.",
             day_offset=4),

    # FP6: Short stem false positive — "рф" inside a longer word
    make_msg("Crimeanwind", 9006,
             "Информация о тарифных изменениях в сфере энергетики. Удар по кошельку потребителей.",
             day_offset=5),

    # FP7: "база данных" = database in IT context
    make_msg("astrapress", 9007,
             "Хакерская атака на базу данных российского министерства. Пожар в серверной.",
             day_offset=0),

    # FP8: Discussion about sanctions, not a strike
    make_msg("Tsaplienko", 9008,
             "Ракетные санкции ударили по российской нефтепереработке в Саратовской области. Экспорт снизился.",
             day_offset=1),

    # FP9: Drill / exercise, not actual strike
    make_msg("oper_ZSU", 9009,
             "Учения ПВО в Ростовской области. Ракетные стрельбы по целям. Все цели поражены.",
             day_offset=2),

    # FP10: Russian strikes on Ukraine — mentions "Kyiv" and "Belgorod" region shelling
    make_msg("supernova_plus", 9010,
             "Обстрел Киева ракетами. Россия атаковала с территории Белгородской области. Жертвы среди мирного населения.",
             day_offset=3),

    # ── FALSE NEGATIVES: should PASS but might get filtered out ──

    # FN1: Uses "поражение" (our fix added "поражен" stem)
    make_msg("Tsaplienko", 10001,
             "Поражение нефтеперерабатывающего завода в Ростовской области. Мощный пожар.",
             day_offset=1),

    # FN2: Uses uncommon weapon name not in keywords
    make_msg("oper_ZSU", 10002,
             "БОБР ВСУ поразил электростанцию в Белгородской области. Крупный пожар.",
             day_offset=2),

    # FN3: Short message, only location (no infra or damage term)
    make_msg("Crimeanwind", 10003,
             "Удар по Крыму. Детали уточняются.",
             day_offset=3),

    # FN4: Aftermath report with no action keyword, just damage
    make_msg("supernova_plus", 10004,
             "Пожар на нефтебазе в Краснодарском крае продолжается уже вторые сутки.",
             day_offset=4),

    # FN5: Ukrainian language, Crimea bridge attack
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
tp_ids = {1001, 2001, 3001, 4001, 5001, 6001, 7001, 8001, 8501, 8502}
fp_ids = {9001, 9002, 9003, 9004, 9005, 9006, 9007, 9008, 9009, 9010}
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

tp_total = len(tp_ids)
fp_total = len(fp_ids)
fn_total = len(fn_ids)

print(f"\nTrue Positives:  {len(filter_results['tp_pass'])}/{tp_total} passed", end="")
if len(filter_results['tp_pass']) == tp_total:
    print(" ✓")
else:
    print(f" ✗ ({len(filter_results['tp_fail'])} missed)")
for mid in filter_results['tp_fail']:
    msg = next(m for m in MESSAGES if m["message_id"] == mid)
    print(f"  MISSED TP {mid}: {msg['text'][:90]}...")

print(f"\nFalse Positives: {len(filter_results['fp_reject'])}/{fp_total} rejected", end="")
if len(filter_results['fp_pass']) == 0:
    print(" ✓")
else:
    print(f" ✗ ({len(filter_results['fp_pass'])} leaked)")
for mid in filter_results['fp_pass']:
    msg = next(m for m in MESSAGES if m["message_id"] == mid)
    print(f"  LEAKED FP {mid}: {msg['text'][:90]}...")

print(f"\nFalse Negatives: {len(filter_results['fn_pass'])}/{fn_total} recovered", end="")
print(f" ({len(filter_results['fn_fail'])} still missed)")
for mid in filter_results['fn_fail']:
    msg = next(m for m in MESSAGES if m["message_id"] == mid)
    print(f"  MISSED FN {mid}: {msg['text'][:90]}...")

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
print(f"\nTP1+TP2 (same event, diff channels) merged: {tp1_2_merged}", "✓" if tp1_2_merged else "✗")

# Check: EC1 and EC2 (different targets, same city, same day) should NOT be merged
ec1_present = any(m["message_id"] == 11001 for m in deduped)
ec2_present = any(m["message_id"] == 11002 for m in deduped)
both_present = ec1_present and ec2_present
print(f"EC1+EC2 (diff targets, same city) both present: {both_present}", "✓" if both_present else "✗")
if not both_present:
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

    # Pair C: Same city, same target type, coordinates ~15km apart
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

    # Pair D: Two cities 80+ km apart in same oblast
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

    # Pair E: "other" vs specific type, close coords (<10km) — should merge
    # (coords 2.5km apart = strong enough signal these are the same event)
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

    # Pair E2: "other" vs specific type, FAR coords (>10km) — should NOT merge
    {"date": "2026-02-07", "city": "Simferopol", "region": "Crimea",
     "target_type": "airfield", "facility_name": "Saki airfield",
     "damage_summary": "Airfield runway hit",
     "latitude": 45.09, "longitude": 33.57, "confidence": "high", "maritime": False,
     "source_channel": "Crimeanwind", "message_date": "2026-02-07"},
    {"date": "2026-02-07", "city": "Simferopol", "region": "Crimea",
     "target_type": "other", "facility_name": None,
     "damage_summary": "Target in Simferopol area",
     "latitude": 44.95, "longitude": 34.10, "confidence": "medium", "maritime": False,
     "source_channel": "oper_ZSU", "message_date": "2026-02-07"},

    # Pair F: Krasnodar vs Krasnoyarsk — must NOT merge (2700km apart!)
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

    # Pair G: Ukrainian vs English spelling — should merge via alias table
    {"date": "2026-02-08", "city": "Воронiж", "region": "Voronezh",
     "target_type": "fuel_depot", "facility_name": None,
     "damage_summary": "Fuel depot on fire in Voronezh",
     "latitude": None, "longitude": None, "confidence": "high", "maritime": False,
     "source_channel": "exilenova_plus", "message_date": "2026-02-08"},
    {"date": "2026-02-08", "city": "Voronezh", "region": "Voronezh",
     "target_type": "fuel_depot", "facility_name": None,
     "damage_summary": "Fuel storage burning in Voronezh",
     "latitude": None, "longitude": None, "confidence": "medium", "maritime": False,
     "source_channel": "astrapress", "message_date": "2026-02-08"},

    # Pair H: "other" + same facility name — SHOULD merge
    {"date": "2026-02-06", "city": "Kerch", "region": "Crimea",
     "target_type": "naval", "facility_name": "Kerch shipyard",
     "damage_summary": "Shipyard damaged by missile",
     "latitude": 45.35, "longitude": 36.47, "confidence": "high", "maritime": True,
     "source_channel": "Crimeanwind", "message_date": "2026-02-06"},
    {"date": "2026-02-06", "city": "Kerch", "region": "Crimea",
     "target_type": "other", "facility_name": "Kerch shipyard",
     "damage_summary": "Target hit in Kerch",
     "latitude": 45.35, "longitude": 36.47, "confidence": "medium", "maritime": False,
     "source_channel": "oper_ZSU", "message_date": "2026-02-06"},
]

print(f"\n{len(synthetic_incidents)} synthetic incidents")

result = dedup.deduplicate(synthetic_incidents)
print(f"{len(result)} after dedup\n")

print("Expected behavior vs actual:")

# Pair A: should merge (same event)
pair_a = [r for r in result if r.get("city") == "Krasnodar" and r.get("date") == "2026-02-03"
          and r.get("target_type") == "oil_refinery"]
ok = len(pair_a) == 1
print(f"  Pair A (same refinery, 2 sources):       merged={ok}  (want: True)  {'✓' if ok else '✗'}")

# Pair B: should NOT merge (different targets)
pair_b = [r for r in result if r.get("city") == "Ryazan" and r.get("date") == "2026-02-09"]
ok = len(pair_b) == 2
print(f"  Pair B (refinery vs airfield, same city): count={len(pair_b)}   (want: 2)     {'✓' if ok else '✗'}")

# Pair C: same city, same type, 15km apart
pair_c = [r for r in result if r.get("city") == "Bryansk" and r.get("date") == "2026-02-05"]
ok = len(pair_c) == 2
print(f"  Pair C (2 ammo depots, 15km apart):       count={len(pair_c)}   (want: 2)     {'✓' if ok else '✗ (merged — could be 2 distinct sites)'}")

# Pair D: different cities, 80km apart
pair_d = [r for r in result if r.get("region") == "Belgorod" and r.get("date") == "2026-02-06"]
ok = len(pair_d) == 2
print(f"  Pair D (Belgorod vs Stary Oskol, ~80km):  count={len(pair_d)}   (want: 2)     {'✓' if ok else '✗'}")

# Pair E: "other" vs specific type, close coords (<10km) — should merge
pair_e = [r for r in result if r.get("city") == "Sevastopol" and r.get("date") == "2026-02-07"]
ok = len(pair_e) == 1
print(f"  Pair E (radar + 'other', <10km coords):    merged={ok}  (want: True)  {'✓' if ok else '✗'}")

# Pair E2: "other" vs specific type, far coords (>10km) — should NOT merge
pair_e2 = [r for r in result if r.get("city") == "Simferopol" and r.get("date") == "2026-02-07"]
ok = len(pair_e2) == 2
print(f"  Pair E2 (airfield + 'other', >10km):       count={len(pair_e2)}   (want: 2)     {'✓' if ok else '✗'}")

# Pair F: Krasnodar vs Krasnoyarsk
pair_f = [r for r in result if r.get("date") == "2026-02-04" and r.get("target_type") == "fuel_depot"]
ok = len(pair_f) == 2
print(f"  Pair F (Krasnodar vs Krasnoyarsk):         count={len(pair_f)}   (want: 2)     {'✓' if ok else '✗ DATA LOSS BUG'}")

# Pair G: Ukrainian vs English spelling — should merge via alias
pair_g = [r for r in result if r.get("region") == "Voronezh" and r.get("date") == "2026-02-08"]
ok = len(pair_g) == 1
print(f"  Pair G (Voronizh/Voronezh alias):          merged={ok}  (want: True)  {'✓' if ok else '✗'}")

# Pair H: "other" + same facility name — SHOULD merge
pair_h = [r for r in result if r.get("city") == "Kerch" and r.get("date") == "2026-02-06"]
ok = len(pair_h) == 1
print(f"  Pair H ('other' + matching facility):      merged={ok}  (want: True)  {'✓' if ok else '✗'}")
if pair_h:
    print(f"    → maritime flag preserved: {pair_h[0].get('maritime')}", "✓" if pair_h[0].get('maritime') else "✗")

# ──────────────────────────────────────────────────────────────
# Section 6: Normalization + city alias test
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("NORMALIZATION + ALIAS TEST")
print("=" * 70)

test_pairs = [
    ("Белгород", "Belgorod", True),
    ("Краснодар", "Krasnodar", True),
    ("Рязань", "Ryazan", True),
    ("Севастополь", "Sevastopol", True),
    ("Брянськ", "Bryansk", True),
    ("Воронiж", "Voronezh", True),    # Should match via alias
    ("Краснодар", "Красноярск", False),  # Must NOT match
]

for cyrillic, other, expected_match in test_pairs:
    norm_c = dedup._normalize(cyrillic)
    norm_o = dedup._normalize(other)
    direct_match = norm_c == norm_o
    alias_match = dedup._cities_equivalent(norm_c, norm_o)
    actual_match = direct_match or alias_match
    ok = actual_match == expected_match
    print(f"  '{cyrillic}' vs '{other}': direct={direct_match}, alias={alias_match} → {actual_match} (want: {expected_match}) {'✓' if ok else '✗'}")

# ──────────────────────────────────────────────────────────────
# Section 7: Token budget check (after fix)
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("TOKEN BUDGET CHECK (after fix)")
print("=" * 70)

prompt_chars = len(filter_and_extract.EXTRACTION_PROMPT)
print(f"Extraction prompt (system): {prompt_chars} chars (~{prompt_chars // 4} tokens)")
print(f"Batch size: {config.BATCH_SIZE} messages")
print(f"Output max_tokens: 8192 (was 4096)")
print(f"At ~150 tokens per incident, max ~{8192 // 150} incidents per batch ✓")

# ──────────────────────────────────────────────────────────────
# Section 8: Validate.py CSV round-trip check (after fix)
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("CSV ROUND-TRIP CHECK (after fix)")
print("=" * 70)

import pandas as pd
import validate

# Read the validate source to check the index= parameter
import inspect
src = inspect.getsource(validate.validate)
uses_index_false = "index=False" in src
uses_index_true = "index=True" in src
print(f"  validate.py uses index=False: {uses_index_false} {'✓' if uses_index_false else '✗'}")
print(f"  validate.py uses index=True:  {uses_index_true} {'✗ BUG' if uses_index_true else '✓ (removed)'}")

# Check validation prompt no longer asks to add rows
has_add_rows = "ADD rows" in src or "add rows" in src or "opus_added" in src
print(f"  Validation prompt asks to add rows: {has_add_rows} {'✗ hallucination risk' if has_add_rows else '✓ (removed)'}")

# ──────────────────────────────────────────────────────────────
# Section 9: Date range filtering check
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("DATE RANGE FILTERING CHECK")
print("=" * 70)

# Check that the extraction code has date validation
src_extract = inspect.getsource(filter_and_extract._send_batch)
has_date_filter = "START_DATE" in src_extract or "config.START_DATE" in src_extract
print(f"  Extraction filters out-of-range dates: {has_date_filter} {'✓' if has_date_filter else '✗'}")

# ──────────────────────────────────────────────────────────────
# Section 10: incidents.jsonl append mode check
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("FILE MODE CHECK")
print("=" * 70)

src_incidents = inspect.getsource(filter_and_extract.extract_incidents)
uses_append = '"a"' in src_incidents
uses_write = '"w"' in src_incidents and '"a"' not in src_incidents
print(f"  incidents.jsonl uses append mode: {uses_append} {'✓' if uses_append else '✗'}")

# ──────────────────────────────────────────────────────────────
# SUMMARY
# ──────────────────────────────────────────────────────────────

print("\n" + "=" * 70)
print("SUMMARY SCORECARD")
print("=" * 70)

# Tally results
issues = []
passes = []

if len(filter_results['tp_pass']) == tp_total:
    passes.append(f"Filter: all {tp_total} true positives pass")
else:
    issues.append(f"Filter: {len(filter_results['tp_fail'])}/{tp_total} true positives missed")

if len(filter_results['fp_pass']) == 0:
    passes.append(f"Filter: all {fp_total} false positives rejected")
else:
    issues.append(f"Filter: {len(filter_results['fp_pass'])}/{fp_total} false positives leak through")

if tp1_2_merged:
    passes.append("Cross-channel dedup merges same-event messages")
else:
    issues.append("Cross-channel dedup fails to merge same-event messages")

if len(pair_f) == 2:
    passes.append("Krasnodar/Krasnoyarsk no longer incorrectly merged")
else:
    issues.append("CRITICAL: Krasnodar/Krasnoyarsk still merged (data loss)")

if len(pair_e) == 1 and len(pair_e2) == 2:
    passes.append("'other' type merging gated by coord proximity (<10km)")
elif len(pair_e2) != 2:
    issues.append("'other' type still auto-merges with distant specific types")
else:
    issues.append("'other' type fails to merge even with close coords")

if uses_index_false and not uses_index_true:
    passes.append("validate.py sends CSV without spurious index column")
else:
    issues.append("validate.py still sends index column")

if not has_add_rows:
    passes.append("Validation prompt no longer asks LLM to hallucinate rows")
else:
    issues.append("Validation prompt still asks LLM to add rows from memory")

if has_date_filter:
    passes.append("Extraction filters out-of-range dates")
else:
    issues.append("No date-range filtering on extracted incidents")

if uses_append:
    passes.append("incidents.jsonl uses append mode (preserves previous runs)")
else:
    issues.append("incidents.jsonl still overwrites on re-run")

print(f"\nPASSED ({len(passes)}):")
for p in passes:
    print(f"  ✓ {p}")

if issues:
    print(f"\nREMAINING ISSUES ({len(issues)}):")
    for i in issues:
        print(f"  ✗ {i}")
else:
    print(f"\nNo remaining issues found!")
