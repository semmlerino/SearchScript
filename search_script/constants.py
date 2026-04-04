# Line content truncation (applies to both small-file and mmap paths)
LINE_CONTENT_MAX_CHARS = 2000

# File size threshold for switching from read() to mmap
LARGE_FILE_MMAP_THRESHOLD = 1024 * 1024  # 1 MB

# Inventory / caching
INVENTORY_CACHE_TTL_S = 600.0  # in-memory freshness window (10 minutes)
PERSISTENT_INDEX_MAX_AGE_S = 600  # SQLite index base max age (10 minutes)
PERSISTENT_INDEX_MAX_AGE_CEILING_S = 14400  # adaptive TTL cap (4 hours)
ADAPTIVE_TTL_SCAN_THRESHOLD_S = 1.0  # scans faster than this use base TTL
ADAPTIVE_TTL_DIVISOR = 2.5  # scaling factor for slow-scan TTL
SPOT_CHECK_SAMPLE_SIZE = 30  # files to stat-check before full rescan
INVENTORY_CACHE_MAX_ENTRIES = 20  # max in-memory cache slots

# Progress reporting intervals
INVENTORY_PROGRESS_MILESTONE = 250  # report every N files during inventory walk
RIPGREP_PROGRESS_MILESTONE = 1000  # report every N matches in ripgrep stream

# Fuzzy matching — tuned empirically for filename search quality
# 78.0 = acceptable partial match (fuzz.partial_ratio)
# 80.0 = required for full-string ratio to qualify
FUZZY_PARTIAL_THRESHOLD = 78.0
FUZZY_FULL_THRESHOLD = 80.0
FUZZY_EXACT_BONUS = 4.0  # boost for exact-substring containment
FUZZY_WORD_BONUS = 2.0  # boost for whole-word boundary match

# Worker thread scaling
DEFAULT_MAX_WORKERS_CAP = 48  # max threads for I/O-bound parallel search

# Parallel content search
CONTENT_SEARCH_POOL_CHUNK_SIZE = 200
INVENTORY_WALK_MAX_WORKERS = 56  # parallel directory walk threads

# Result queue / UI update timing
RESULT_BATCH_SIZE = 100
RESULT_FIRST_BATCH_SIZE = 15
PROCESS_RESULTS_TIME_BUDGET_S = 0.025  # 25 ms per QTimer tick (~1 frame budget)
RESULT_POLL_INITIAL_DELAY_MS = 100
RESULT_POLL_BACKOFF_DELAY_MS = 50
