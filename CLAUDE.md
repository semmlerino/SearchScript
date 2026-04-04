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

MVC pattern with callback-based communication (no Qt signals between layers):

- **`SearchController`** — orchestrator. Wires UI callbacks to search operations, manages threading (daemon thread + `QTimer.singleShot` polling a `queue.Queue` for results), tracks search history.
- **`SearchUI`** (`QMainWindow`) — pure view. Exposes callback slots (`on_search_start`, `on_search_cancel`, etc.) that the controller assigns. Owns all widgets and export logic.
- **`SearchEngine`** — core search logic. Generator-based (`yield SearchResult`), supports cancellation via `threading.Event`, uses `mmap` for files >1MB. Search modes defined in `SearchMode` enum.
- **`config.py`** — `SearchConfig` dataclass + `ConfigManager` (JSON persistence) + custom exception hierarchy (`SearchError`, `DirectoryError`, `FileAccessError`, `ValidationError`).
- **`file_utils.py`** — `FileOperations` (platform-aware file/folder opening), `LoggingConfig`, `ValidationUtils`.

## Key Patterns

- Search runs on a daemon thread; results flow back via `queue.Queue` polled by `QTimer.singleShot(100, ...)` on the main thread. No direct Qt calls from worker threads.
- `SearchEngine.search_files()` is a generator yielding `SearchResult` objects. The controller collects all results before displaying (not streamed to UI incrementally).
- File type filtering uses extension string matching (not MIME types). Binary extensions are hardcoded in `SearchEngine._always_binary_extensions`.
- Fuzzy matching threshold is hardcoded at 70 (`fuzz.partial_ratio`).

## Testing

Tests cover `SearchEngine`, `FileOperations`, `ValidationUtils`, and `ConfigManager`. No UI tests — `SearchUI` requires a `QApplication` and is not tested. Tests use `tmp_path` fixtures for filesystem isolation.
