# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PySide6 GUI application for searching files by name or content, with support for substring, glob, regex, and fuzzy (rapidfuzz) search modes. Entry point: `file-search` CLI command.

## Commands

```bash
uv run file-search                     # Run the application
uv run pytest                          # Run all tests
uv run pytest tests/test_search.py::test_filename_search  # Run a single test
uv run ruff check . --fix              # Lint
uv run ruff format .                   # Format
uv run basedpyright                    # Type check
```

## Architecture

MVC pattern. Thread results are delivered via `queue.Queue` polled by `QTimer`, not Qt signals. `ui_components.py` uses signals internally for widget communication.

| File | Owns |
|---|---|
| `main.py` | CLI entry point. Connects `app.aboutToQuit` to `search_engine.shutdown()` — required for clean teardown. |
| `search_controller.py` | `SearchController` — orchestrator. Wires UI callbacks to search operations, manages the worker thread and result queue, tracks search history. |
| `ui_components.py` | `SearchUI (QMainWindow)` — pure view. Exposes callback slots (`on_search_start`, `on_search_cancel`, etc.) that the controller assigns. Owns all widgets and export logic. |
| `search_engine.py` | `SearchEngine` — core search logic. Generator-based (`yield SearchResult`), supports cancellation via `threading.Event`, uses `mmap` for files >1 MB. Delegates to `RipgrepBackend` for non-fuzzy searches when `rg` is available. |
| `ripgrep_backend.py` | `RipgrepBackend` — wraps `rg` subprocess for both content and filename search (`search` and `search_filenames`), parses JSON output, builds type/depth/context flags. Auto-detected via `shutil.which("rg")` at startup; falls back to Python backend for fuzzy mode or when `rg` is absent. |
| `inventory.py` | `InventoryManager` — dual-layer file cache (see Caching below). |
| `search_index.py` | `SearchIndexStore` — SQLite persistence backing the inventory cache. Also defines `InventoryCacheKey`, `InventoryEntry`, `InventorySnapshot`, `InventoryLoadResult` dataclasses. |
| `models.py` | Dataclasses and enums: `SearchResult`, `SearchMode`, `SearchBackend`, `MatchPlan`, `SearchParams`, queue message types (`ResultBatchMsg`, `DoneMsg`, `ErrorMsg`, `CancelledMsg`, `StatusMsg`, `LimitReachedMsg`), and helpers (`check_file_filters`, `truncate_line`, `ensure_glob_wildcard`). |
| `constants.py` | All tunable thresholds and sizes (TTLs, batch sizes, fuzzy thresholds, etc.). |
| `config.py` | Custom exception hierarchy: `SearchError`, `DirectoryError`, `FileAccessError`, `ValidationError`. |
| `file_utils.py` | `FileOperations` (platform-aware file/folder opening), `LoggingConfig`. |

## Key Patterns

- Search runs on a daemon thread. Results flow back via `queue.Queue` polled by `QTimer.singleShot`: initial delay 100 ms, then 0 ms when the queue has items, 50 ms when idle (`RESULT_POLL_INITIAL_DELAY_MS` / `RESULT_POLL_BACKOFF_DELAY_MS` in `constants.py`). No direct Qt calls from worker threads.
- `SearchEngine.search_files()` is a generator yielding `SearchResult` objects. The controller streams results to the UI in batches: first batch at 15 results, subsequent batches at 100, with a 25 ms per-frame time budget to keep the UI responsive.
- File type filtering uses extension string matching (not MIME types). Binary detection uses a three-tier model: `_always_binary_extensions` (VFX formats like `.abc`, `.vdb`, `.exr`, `.hip` — skipped unconditionally for content search), `_maybe_binary_extensions` (e.g. `.usd` — sniffed for null bytes in the first 8 KB), and everything else (assumed text).
- Gitignore filtering: `InventoryManager` uses `pathspec` to parse `.gitignore` files, including nested `.gitignore` in subdirectories. Controlled by the `include_ignored` parameter (default `True` = ignore `.gitignore` rules). When `False`, gitignore specs accumulate as the directory walk descends.
- Fuzzy matching thresholds are defined in `constants.py`: `FUZZY_PARTIAL_THRESHOLD = 78.0` and `FUZZY_FULL_THRESHOLD = 80.0`.
- `SearchParams` exposes additional search controls: `context_lines`, `case_sensitive`, `follow_symlinks`, `match_folders`, `exclude_shots`, `include_ignored`.

## Optional Dependencies

- `rapidfuzz` — required for fuzzy search mode. Install via `uv pip install -e ".[fuzzy]"`. Without it, fuzzy mode is unavailable.
- `chardet` — undeclared optional. If importable, `SearchEngine` uses it for encoding detection. Falls back to heuristic BOM detection otherwise.

## Caching Architecture

Filename searches use a two-level cache to avoid repeated filesystem walks:

- **L1 (in-memory):** Up to 6 entries, TTL controlled by `INVENTORY_CACHE_TTL_S` (60 s).
- **L2 (SQLite):** Up to 12 entries at `~/.cache/file-search/index.db`. TTL starts at `PERSISTENT_INDEX_MAX_AGE_S` (300 s) and scales adaptively up to `PERSISTENT_INDEX_MAX_AGE_CEILING_S` (3600 s) based on how long the initial scan took.
- Before reusing a stale L2 snapshot, the manager spot-checks the 30 most recently modified files.
- When a cache entry is stale but usable, a background thread refreshes it while the old results are returned immediately.

## Testing

Tests cover `SearchEngine` (all search modes, mmap path, binary detection, gitignore filtering), `SearchController` (batching, draining, date filters), `SearchUI` validation and widget behavior, `InventoryManager` (caching, TTL scaling, spot-checks, background refresh), `SearchIndexStore` (persistence, schema migration, transaction safety), and `RipgrepBackend` (content and filename search). A `qapp` fixture provides an offscreen `QApplication` for controller and UI tests. Tests use `tmp_path` fixtures for filesystem isolation.
