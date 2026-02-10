import json
import math
import os
from datetime import datetime

import pandas as pd

import config

# Cyrillic → Latin transliteration table for normalization
_TRANSLIT = str.maketrans({
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "kh", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "shch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    # Ukrainian extras
    "і": "i", "ї": "yi", "є": "ye", "ґ": "g",
    "'": "", "'": "", "ʼ": "", "`": "",
})


def _normalize(s: str) -> str:
    """Normalize a string: lowercase, transliterate Cyrillic, strip punctuation."""
    if not s:
        return ""
    s = s.lower().strip()
    s = s.translate(_TRANSLIT)
    # Remove remaining non-alphanumeric except spaces
    s = "".join(c if c.isalnum() or c == " " else "" for c in s)
    # Collapse whitespace
    return " ".join(s.split())


# Known city name equivalences (transliteration variants that don't match
# after simple character-level transliteration).
_CITY_ALIASES = {}
for _group in [
    ["voronezh", "voronizh"],
    ["sevastopol", "sevastopil"],
    ["zaporizhzhia", "zaporizhzhya", "zaporozhye", "zaporozhe"],
    ["dnipro", "dnepr"],
    ["kharkiv", "kharkov", "kharkow"],
    ["kyiv", "kiev", "kiyev"],
    ["odesa", "odessa"],
    ["mykolaiv", "nikolaev", "mikolayiv"],
    ["dzhankoi", "dzhankoy"],
    ["feodosia", "feodosiya", "feodosija"],
    ["yevpatoria", "yevpatoriya", "evpatoria"],
    ["simferopol", "simferopil"],
    ["bryansk", "briansk"],
    ["ryazan", "ryazan"],
    ["krasnodar", "krasnodar"],
    ["krasnoyarsk", "krasnoyarsk"],
    ["saratov", "saratov"],
    ["yekaterinburg", "ekaterinburg"],
]:
    canonical = _group[0]
    for _name in _group:
        _CITY_ALIASES[_name] = canonical


def _cities_equivalent(a: str, b: str) -> bool:
    """Check if two normalized city names refer to the same city."""
    ca = _CITY_ALIASES.get(a, a)
    cb = _CITY_ALIASES.get(b, b)
    return ca == cb


def _parse_date(date_str: str) -> datetime | None:
    """Parse a date string to datetime."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, IndexError):
        return None


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance between two coordinates in km."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _locations_match(a: dict, b: dict) -> bool:
    """Check if two incidents refer to the same location."""
    # Coordinate check (30km radius — tight enough to avoid merging
    # different facilities in the same oblast)
    if (a.get("latitude") and a.get("longitude") and
            b.get("latitude") and b.get("longitude")):
        try:
            dist = _haversine_km(
                float(a["latitude"]), float(a["longitude"]),
                float(b["latitude"]), float(b["longitude"]),
            )
            if dist < 30:
                return True
        except (ValueError, TypeError):
            pass

    # Normalized city match
    city_a = _normalize(a.get("city", ""))
    city_b = _normalize(b.get("city", ""))

    if city_a and city_b:
        # Exact match after normalization
        if city_a == city_b:
            return True
        # Also try known equivalences (Ukrainian vs English spelling)
        if _cities_equivalent(city_a, city_b):
            return True
        # Substring match only when the shorter string is long enough
        # to be meaningful (avoids "kov" matching "kharkov")
        shorter = min(len(city_a), len(city_b))
        if shorter >= 6 and (city_a in city_b or city_b in city_a):
            return True

    # Same region + similar facility name
    region_a = _normalize(a.get("region", ""))
    region_b = _normalize(b.get("region", ""))
    facility_a = _normalize(a.get("facility_name", ""))
    facility_b = _normalize(b.get("facility_name", ""))

    if region_a and region_b and region_a == region_b:
        if facility_a and facility_b and (facility_a in facility_b or facility_b in facility_a):
            return True

    return False


def _same_target_type(a: dict, b: dict) -> bool:
    """Check if target types are compatible for merging."""
    ta = (a.get("target_type") or "other").lower()
    tb = (b.get("target_type") or "other").lower()
    if ta == tb:
        return True
    # "other" can only merge with a specific type if they share the same
    # facility name or coordinates are very close (<10km) — prevents
    # vague "military target" from swallowing unrelated specific targets.
    if ta == "other" or tb == "other":
        # Check for coordinate proximity as confirmation
        if (a.get("latitude") and a.get("longitude") and
                b.get("latitude") and b.get("longitude")):
            try:
                dist = _haversine_km(
                    float(a["latitude"]), float(a["longitude"]),
                    float(b["latitude"]), float(b["longitude"]),
                )
                if dist < 10:
                    return True
            except (ValueError, TypeError):
                pass
        # Check for matching facility name
        fa = _normalize(a.get("facility_name", ""))
        fb = _normalize(b.get("facility_name", ""))
        if fa and fb and (fa in fb or fb in fa):
            return True
        return False
    return False


def _event_dates_close(a: dict, b: dict, max_days: int = 2) -> bool:
    """Check if two incidents' EVENT dates are within max_days."""
    da = _parse_date(a.get("date", ""))
    db = _parse_date(b.get("date", ""))
    if not da or not db:
        return False
    return abs((da - db).days) <= max_days


def _merge_cluster(cluster: list[dict]) -> dict:
    """Merge a cluster of duplicate incidents into one."""
    # Pick the most detailed record as base (longest damage summary)
    cluster.sort(key=lambda x: len(x.get("damage_summary", "") or ""), reverse=True)
    merged = dict(cluster[0])

    # --- Date handling ---
    # Event date: use the earliest across the cluster
    event_dates = sorted(
        (inc.get("date", "") for inc in cluster if inc.get("date")),
        key=lambda d: d,
    )
    if event_dates:
        merged["date"] = event_dates[0]                  # earliest event date
        merged["last_event_date"] = event_dates[-1]      # latest event date

    # Message timestamps: track first and last
    msg_dates = sorted(
        (inc.get("message_date", "") for inc in cluster if inc.get("message_date")),
        key=lambda d: d,
    )
    if msg_dates:
        merged["first_message_date"] = msg_dates[0]
        merged["last_message_date"] = msg_dates[-1]

    # Collect all source channels
    channels = set()
    for inc in cluster:
        for ch in (inc.get("source_channel") or "").split(", "):
            ch = ch.strip()
            if ch:
                channels.add(ch)
    merged["source_channel"] = ", ".join(sorted(channels))

    # Use highest confidence
    conf_order = {"high": 3, "medium": 2, "low": 1}
    best_conf = max(cluster, key=lambda x: conf_order.get(
        str(x.get("confidence", "low")).lower(), 0))
    merged["confidence"] = best_conf.get("confidence", "low")

    # Use most specific coordinates (non-null, from highest confidence entry)
    for inc in sorted(cluster, key=lambda x: conf_order.get(
            str(x.get("confidence", "low")).lower(), 0), reverse=True):
        if inc.get("latitude") and inc.get("longitude"):
            merged["latitude"] = inc["latitude"]
            merged["longitude"] = inc["longitude"]
            break

    # Maritime flag: true if any entry in cluster is maritime
    merged["maritime"] = any(inc.get("maritime") is True or inc.get("maritime") == "true"
                            for inc in cluster)

    # Drop internal fields
    merged.pop("source_message_id", None)
    merged.pop("_source_channels", None)
    merged.pop("message_date", None)

    return merged


def deduplicate(incidents: list[dict]) -> list[dict]:
    """Cluster and merge duplicate incidents."""
    if not incidents:
        return []

    # Drop low-confidence entries
    before = len(incidents)
    incidents = [i for i in incidents if str(i.get("confidence", "")).lower() != "low"]
    dropped = before - len(incidents)
    if dropped:
        print(f"  Dropped {dropped} low-confidence entries")

    # Sort by event date
    incidents.sort(key=lambda x: x.get("date", ""))

    # Union-Find
    parent = list(range(len(incidents)))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    # Compare pairs within event date window
    for i in range(len(incidents)):
        for j in range(i + 1, len(incidents)):
            di = _parse_date(incidents[i].get("date", ""))
            dj = _parse_date(incidents[j].get("date", ""))
            if di and dj and (dj - di).days > 2:
                break
            if not _event_dates_close(incidents[i], incidents[j]):
                continue
            if _locations_match(incidents[i], incidents[j]) and \
               _same_target_type(incidents[i], incidents[j]):
                union(i, j)

    # Group by cluster
    clusters: dict[int, list[dict]] = {}
    for i, inc in enumerate(incidents):
        root = find(i)
        clusters.setdefault(root, []).append(inc)

    # Merge each cluster
    deduplicated = [_merge_cluster(c) for c in clusters.values()]
    deduplicated.sort(key=lambda x: x.get("date", ""))

    return deduplicated


def run(incidents: list[dict] | None = None) -> list[dict]:
    """Run deduplication."""
    if incidents is None:
        incidents = []
        path = os.path.join(config.EXTRACTED_DIR, "incidents.jsonl")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            incidents.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue

    print(f"Deduplicating {len(incidents)} incidents...")
    deduplicated = deduplicate(incidents)
    print(f"  {len(deduplicated)} unique incidents after dedup.")
    return deduplicated


def to_csv(incidents: list[dict], output_path: str | None = None):
    """Export incidents to CSV."""
    if not output_path:
        output_path = config.OUTPUT_CSV

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    columns = [
        "date", "city", "region", "facility_name", "target_type",
        "damage_summary", "latitude", "longitude", "source_channel",
        "confidence", "maritime",
        "first_message_date", "last_message_date", "last_event_date",
    ]

    df = pd.DataFrame(incidents)
    for col in columns:
        if col not in df.columns:
            df[col] = None
    # Normalize maritime to boolean
    df["maritime"] = df["maritime"].apply(lambda x: True if x is True or x == "true" else False)

    df = df[columns].rename(columns={
        "date": "Date",
        "city": "City",
        "region": "Region",
        "facility_name": "Facility Name",
        "target_type": "Target Type",
        "damage_summary": "Damage Summary",
        "latitude": "Latitude",
        "longitude": "Longitude",
        "source_channel": "Source Channel",
        "confidence": "Confidence",
        "maritime": "Maritime",
        "first_message_date": "First Message Date",
        "last_message_date": "Last Message Date",
        "last_event_date": "Last Event Date",
    })

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"  CSV saved to {output_path} ({len(df)} rows)")
