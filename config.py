from datetime import datetime

# Telegram channels to scrape
CHANNELS = [
    "Crimeanwind",
    "Tsaplienko",
    "exilenova_plus",
    "supernova_plus",
    "astrapress",
    "oper_ZSU",
]

# Date range (TEST RUN: January 2026 only — change back for full scrape)
START_DATE = datetime(2026, 1, 1)
END_DATE = datetime(2026, 2, 1)  # exclusive upper bound

# --- PRE-FILTER KEYWORD CATEGORIES ---
# Pre-filter requires: (action term) AND (location OR infrastructure OR damage term)
# This eliminates most irrelevant messages before they reach Claude.

KEYWORDS_ACTION = [
    # Russian
    "удар", "ракет", "дрон", "бпла", "шахед", "атак", "обстрел",
    "прилёт", "прилет", "детонац", "порази", "порази",
    # Ukrainian
    "обстріл", "ракет", "дрон", "бпла", "шахед", "атак",
    "приліт", "уражен", "вдари",
    # English
    "strike", "missile", "drone", "shahed", "attack", "hit",
]

KEYWORDS_DAMAGE = [
    # Russian
    "пожар", "взрыв", "горит", "уничтож", "повреж", "разруш", "сбит",
    "ликвидир", "детонир",
    # Ukrainian
    "пожеж", "вибух", "палає", "знищен", "пошкодж", "зруйнов",
    # English
    "explosion", "fire", "damage", "destroy", "burning",
]

KEYWORDS_INFRASTRUCTURE = [
    # Russian
    "нпз", "нефтебаз", "нефтеперераб", "нефтехим",
    "склад боеприпас", "арсенал", "аэродром",
    "энергообъект", "подстанци", "тэц", "грэс", "электростанц",
    "военн", "воинск", "база", "казарм",
    "рлс", "радар", "пво",
    # Ukrainian
    "нпз", "нафтобаз", "нафтоперероб",
    "склад боєприпас", "арсенал", "аеродром",
    "енергооб", "підстанц", "тец", "грес", "електростанц",
    "військов", "база", "казарм",
    "рлс", "радар", "ппо",
    # English
    "depot", "refinery", "airfield", "airbase", "power plant", "substation",
    "military base", "ammunition", "radar", "air defense", "barracks",
    # Maritime / tanker targets
    "танкер", "tanker", "судно", "vessel", "нефтеналивн",
    "теплоход", "сухогруз", "платформ", "platform",
    "черное море", "чорне море", "black sea",
    "каспийск", "каспійськ", "caspian",
    "азовск", "азовськ", "azov",
]

# Russian locations — presence of these terms boosts relevance.
# Uses short stems for substring matching (e.g. "белгород" matches "белгородская").
KEYWORDS_RUSSIAN_LOCATIONS = [
    # --- Crimea & Sevastopol ---
    "крым", "крим", "crimea",
    "севастопол", "sevastopol",
    "симферопол", "simferopol",
    "керч", "kerch",
    "джанкой", "dzhankoy",
    "феодоси", "feodosia",
    "ялт", "yalta",
    "евпатори", "євпаторі", "yevpatoria",
    "саки", "saki",
    # --- Russian oblasts (all 46) — Russian / Ukrainian / English stems ---
    # Amur
    "амурск", "амурськ", "amur",
    # Arkhangelsk
    "архангельск", "архангельськ", "arkhangelsk",
    # Astrakhan
    "астрахан", "астраханськ", "astrakhan",
    # Belgorod
    "белгород", "бєлгородськ", "belgorod",
    # Bryansk
    "брянск", "брянськ", "bryansk",
    # Chelyabinsk
    "челябинск", "челябінськ", "chelyabinsk",
    # Irkutsk
    "иркутск", "іркутськ", "irkutsk",
    # Ivanovo
    "иванов", "іванівськ", "ivanovo",
    # Kaliningrad
    "калининград", "калінінградськ", "kaliningrad",
    # Kaluga
    "калуж", "калузьк", "kaluga",
    # Kemerovo
    "кемеров", "кемеровськ", "kemerovo",
    # Kirov
    "кировск", "кіровськ", "kirov",
    # Kostroma
    "костром", "костромськ", "kostroma",
    # Kurgan
    "курган", "курганськ", "kurgan",
    # Kursk
    "курск", "курськ", "kursk",
    # Leningrad
    "ленинградск", "ленінградськ", "leningrad",
    # Lipetsk
    "липецк", "липецьк", "lipetsk",
    # Magadan
    "магадан", "магаданськ", "magadan",
    # Moscow
    "москв", "московськ", "moscow",
    # Murmansk
    "мурманск", "мурманськ", "murmansk",
    # Nizhny Novgorod
    "нижегородск", "нижньогородськ", "nizhny novgorod",
    # Novgorod
    "новгородск", "новгородськ", "novgorod",
    # Novosibirsk
    "новосибирск", "новосибірськ", "novosibirsk",
    # Omsk
    "омск", "омськ", "omsk",
    # Orenburg
    "оренбургск", "оренбурзьк", "orenburg",
    # Oryol
    "орловск", "орловськ", "орёл", "орел", "oryol", "orel",
    # Penza
    "пензенск", "пензенськ", "пенз", "penza",
    # Pskov
    "псковск", "псковськ", "pskov",
    # Rostov
    "ростовск", "ростовськ", "ростов", "rostov",
    # Ryazan
    "рязанск", "рязанськ", "рязан", "ryazan",
    # Sakhalin
    "сахалинск", "сахалінськ", "sakhalin",
    # Samara
    "самарск", "самарськ", "самар", "samara",
    # Saratov
    "саратовск", "саратовськ", "саратов", "saratov",
    # Smolensk
    "смоленск", "смоленськ", "smolensk",
    # Sverdlovsk
    "свердловск", "свердловськ", "sverdlovsk", "екатеринбург", "yekaterinburg",
    # Tambov
    "тамбовск", "тамбовськ", "тамбов", "tambov",
    # Tomsk
    "томск", "томськ", "tomsk",
    # Tula
    "тульск", "тульськ", "тул", "tula",
    # Tver
    "тверск", "тверськ", "тверь", "tver",
    # Tyumen
    "тюменск", "тюменськ", "тюмень", "tyumen",
    # Ulyanovsk
    "ульяновск", "ульянівськ", "ulyanovsk",
    # Vladimir
    "владимирск", "владимирськ", "vladimir",
    # Volgograd
    "волгоградск", "волгоградськ", "волгоград", "volgograd",
    # Vologda
    "вологодск", "вологодськ", "вологд", "vologda",
    # Voronezh
    "воронежск", "воронезьк", "воронеж", "voronezh",
    # Yaroslavl
    "ярославск", "ярославськ", "ярославл", "yaroslavl",
    # --- Russian republics ---
    "татарстан", "tatarstan", "казан", "kazan",
    "башкортостан", "bashkortostan", "башкир", "bashkir",
    "дагестан", "dagestan",
    "чечн", "chechnya", "грозн", "grozny",
    "ингушет", "ingushetia",
    "осети", "ossetia", "беслан",
    "кабардин", "kabardino",
    "карачаев", "karachay",
    "адыге", "adygea",
    "крым", "crimea",
    "мордов", "mordovia",
    "удмурт", "udmurt",
    "чуваш", "chuvash",
    "мари эл", "mari el",
    # --- Russian krais ---
    "краснодарск", "krasnodar",
    "ставропольск", "ставропол", "stavropol",
    "красноярск", "krasnoyarsk",
    "пермск", "perm",
    "приморск", "primorsky",
    "хабаровск", "khabarovsk",
    "алтайск", "altai",
    "забайкальск", "zabaykalsky",
    "камчатск", "kamchatka",
    # --- Russian federal cities ---
    "санкт-петербург", "петербург", "st. petersburg", "saint petersburg",
    # --- Generic ---
    "россия", "росія", "russia", "рф",
]

# Ukrainian oblasts (all 24) + Kyiv city — used to exclude messages
# about Russian strikes ON Ukraine (which are not what we want).
KEYWORDS_UKRAINIAN_TARGETS = [
    # Cherkasy
    "черкас", "cherkasy",
    # Chernihiv
    "чернігів", "чернигов", "chernihiv",
    # Chernivtsi
    "чернівц", "черновц", "chernivtsi",
    # Dnipropetrovsk
    "дніпропетровськ", "днепропетровск", "дніпр", "днепр", "dnipro", "dnipropetrovsk",
    # Donetsk (occupied — but messages about Russian strikes on UA-held parts)
    "донецьк", "донецк", "donetsk",
    # Ivano-Frankivsk
    "івано-франківськ", "ивано-франковск", "ivano-frankivsk",
    # Kharkiv
    "харків", "харьков", "kharkiv",
    # Kherson
    "херсон", "kherson",
    # Khmelnytskyi
    "хмельницьк", "хмельницк", "khmelnytskyi",
    # Kirovohrad
    "кіровоград", "кировоград", "kirovohrad",
    # Kyiv
    "київ", "киев", "kyiv", "kiev",
    # Luhansk (occupied — same logic as Donetsk)
    "луганськ", "луганск", "luhansk",
    # Lviv
    "львів", "львов", "lviv",
    # Mykolaiv
    "миколаїв", "николаев", "mykolaiv",
    # Odesa
    "одес", "odesa", "odessa",
    # Poltava
    "полтав", "poltava",
    # Rivne
    "рівне", "ровно", "rivne",
    # Sumy
    "суми", "сумы", "sumy",
    # Ternopil
    "тернопіл", "тернопол", "ternopil",
    # Vinnytsia
    "вінниц", "винниц", "vinnytsia",
    # Volyn
    "волинь", "волынь", "volyn",
    # Zakarpattia
    "закарпатт", "закарпать", "zakarpattia",
    # Zaporizhzhia
    "запоріжж", "запорож", "zaporizhzhia",
    # Zhytomyr
    "житомир", "zhytomyr",
]

# --- PROCESSING SETTINGS ---

# Claude API
CLAUDE_MODEL = "claude-sonnet-4-5-20250929"
BATCH_SIZE = 25           # messages per Claude API call
MAX_RETRIES = 3
RETRY_DELAY = 5           # seconds
MAX_CONCURRENT = 4        # parallel Claude API calls

# Pre-filter
DEDUP_SIMILARITY = 0.7    # cross-channel text dedup threshold (word overlap)

# Output paths
DATA_DIR = "data"
RAW_DIR = f"{DATA_DIR}/raw"
EXTRACTED_DIR = f"{DATA_DIR}/extracted"
OUTPUT_CSV = f"{DATA_DIR}/ukraine_strikes_russia.csv"
