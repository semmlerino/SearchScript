import codecs
import fnmatch
import json
import logging
import mmap
import os
import re
import shutil
import subprocess
import threading
import types
from base64 import b64decode
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from queue import Empty, Queue
from time import time

try:
    from rapidfuzz import fuzz

    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

from .config import DirectoryError, FileAccessError, SearchError, ValidationError
from .constants import (
    FUZZY_EXACT_BONUS,
    FUZZY_FULL_THRESHOLD,
    FUZZY_PARTIAL_THRESHOLD,
    FUZZY_WORD_BONUS,
    INVENTORY_CACHE_MAX_ENTRIES,
    INVENTORY_CACHE_TTL_S,
    INVENTORY_PROGRESS_MILESTONE,
    LARGE_FILE_MMAP_THRESHOLD,
    LINE_CONTENT_MAX_CHARS,
    PERSISTENT_INDEX_MAX_AGE_S,
    RIPGREP_PROGRESS_MILESTONE,
)
from .search_index import (
    InventoryCacheKey,
    InventoryEntry,
    InventorySnapshot,
    SearchIndexStore,
)


@dataclass
class SearchResult:
    file_path: str
    line_number: int | None = None
    line_content: str | None = None
    next_line: str | None = None
    mod_time: float | None = None
    file_size: int | None = None
    match_score: float | None = None

    @property
    def display_text(self) -> str:
        if self.line_number and self.line_content:
            return f"{self.line_number}: {self.line_content}"
        return ""

    @property
    def formatted_mod_time(self) -> str:
        if self.mod_time is not None:
            return datetime.fromtimestamp(self.mod_time).strftime("%Y-%m-%d %H:%M:%S")
        return "N/A"

    @property
    def formatted_size(self) -> str:
        if self.file_size is not None:
            size = float(self.file_size)
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if size < 1024.0:
                    return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
                size /= 1024.0
            return f"{size:.1f} PB"
        return "N/A"


class SearchMode(Enum):
    SUBSTRING = "substring"
    GLOB = "glob"
    REGEX = "regex"
    FUZZY = "fuzzy"


class SearchBackend(Enum):
    AUTO = "auto"
    PYTHON = "python"
    RIPGREP = "ripgrep"


@dataclass(frozen=True)
class MatchPlan:
    mode: SearchMode
    raw_term: str
    normalized_term: str
    regex: re.Pattern[str] | None = None


class SearchEngine:
    BACKGROUND_REFRESH_STATUS = "Using cached file inventory while refreshing index in background"

    def __init__(
        self,
        logger: logging.Logger | None = None,
        max_workers: int = 4,
        index_db_path: str | Path | None = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self._rg_path = shutil.which("rg")
        self._inventory_cache: dict[InventoryCacheKey, InventorySnapshot] = {}
        self._inventory_cache_lock = threading.Lock()
        self._inventory_refreshes: set[InventoryCacheKey] = set()
        self._inventory_refresh_lock = threading.Lock()
        self._index_store = SearchIndexStore(self.logger, db_path=index_db_path)
        self._binary_extensions = {
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".bin",
            ".obj",
            ".o",
            ".a",
            ".lib",
            ".zip",
            ".tar",
            ".gz",
            ".rar",
            ".7z",
            ".bz2",
            ".xz",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".bmp",
            ".tiff",
            ".ico",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
            ".wmv",
            ".flv",
            ".mkv",
            ".webm",
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            # VFX image formats
            ".exr",
            ".dpx",
            ".hdr",
            ".r3d",
            ".ari",
            ".braw",
            # VFX scene and cache formats
            ".abc",
            ".usd",
            ".usdc",
            ".bgeo",
            ".vdb",
            ".mb",
            # Houdini formats
            ".hip",
            ".hipnc",
            # 3D interchange formats
            ".fbx",
            # Audio formats
            ".wav",
            ".aiff",
            # Nuke compiled format
            ".nknc",
        }
        try:
            import chardet  # pyright: ignore[reportMissingImports]

            self._chardet: types.ModuleType | None = chardet
        except ImportError:
            self._chardet = None

    def search_files(
        self,
        directory: str,
        search_term: str,
        include_types: list[str] | None = None,
        exclude_types: list[str] | None = None,
        search_within_files: bool = False,
        search_mode: SearchMode = SearchMode.SUBSTRING,
        search_backend: SearchBackend = SearchBackend.AUTO,
        max_depth: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        max_results: int | None = None,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
        match_folders: bool = False,
        follow_symlinks: bool = False,
        progress_callback=None,
        cancel_event=None,
        on_limit_reached: Callable[[int], None] | None = None,
    ) -> Generator[SearchResult, None, None]:
        """
        Search for files/content matching criteria.

        Args:
            directory: Root directory to search
            search_term: Term to search for
            include_types: File extensions to include (e.g., ['.txt', '.py'])
            exclude_types: File extensions to exclude
            search_within_files: If True, search file contents; if False, search filenames
            follow_symlinks: If True, follow symbolic links during traversal
            progress_callback: Callback function for progress updates
            cancel_event: Threading event to signal cancellation

        Yields:
            SearchResult objects for each match found

        Raises:
            DirectoryError: If directory doesn't exist or isn't accessible
            ValidationError: If search parameters are invalid
            SearchError: For other search-related errors
        """
        # Validate inputs
        self._validate_search_params(directory, search_term)
        if max_results is not None and max_results <= 0:
            raise ValidationError("Max results must be a positive integer")

        include_types = self._normalize_file_types(include_types)
        exclude_types = self._normalize_file_types(exclude_types)
        match_plan = self._build_match_plan(search_term, search_mode)
        modified_after_ts = modified_after.timestamp() if modified_after is not None else None
        modified_before_ts = modified_before.timestamp() if modified_before is not None else None

        backend = self._resolve_search_backend(search_within_files, search_mode, search_backend)
        if backend == SearchBackend.RIPGREP:
            yield from self._search_file_content_with_ripgrep(
                directory=directory,
                match_plan=match_plan,
                include_types=include_types,
                exclude_types=exclude_types,
                max_depth=max_depth,
                min_size=min_size,
                max_size=max_size,
                max_results=max_results,
                modified_after_ts=modified_after_ts,
                modified_before_ts=modified_before_ts,
                follow_symlinks=follow_symlinks,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
                on_limit_reached=on_limit_reached,
            )
            return

        files_processed = 0
        emitted_results = 0
        inventory = self._get_inventory_snapshot(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
        )
        if inventory is None:
            return

        deferred_results: list[SearchResult] | None = (
            [] if not search_within_files and search_mode == SearchMode.FUZZY else None
        )

        self.logger.debug(f"Starting search in directory: {directory}")

        try:
            if match_folders and not search_within_files:
                for dir_path in inventory.directories:
                    if cancel_event and cancel_event.is_set():
                        self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                        return
                    folder_name = os.path.basename(dir_path)
                    score = self._score_match(
                        folder_name,
                        match_plan,
                        allow_partial_fuzzy=False,
                    )
                    if score is None:
                        continue
                    result = SearchResult(dir_path, match_score=score)
                    if deferred_results is not None:
                        deferred_results.append(result)
                    else:
                        yield result
                        emitted_results += 1
                        if max_results is not None and emitted_results >= max_results:
                            if on_limit_reached:
                                on_limit_reached(max_results)
                            return

            for entry in inventory.files:
                if cancel_event and cancel_event.is_set():
                    self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                    return

                if not self._check_cached_inventory_filters(
                    entry,
                    include_types=include_types,
                    exclude_types=exclude_types,
                    search_within_files=search_within_files,
                    min_size=min_size,
                    max_size=max_size,
                    modified_after_ts=modified_after_ts,
                    modified_before_ts=modified_before_ts,
                ):
                    files_processed += 1
                    self._update_cached_inventory_progress(
                        files_processed,
                        len(inventory.files),
                        progress_callback,
                    )
                    continue

                file_path = Path(entry.file_path)
                try:
                    if search_within_files:
                        for result in self._search_file_content(
                            file_path,
                            match_plan,
                            file_size=entry.file_size,
                        ):
                            result.mod_time = entry.mod_time
                            yield result
                            emitted_results += 1
                            if max_results is not None and emitted_results >= max_results:
                                if on_limit_reached:
                                    on_limit_reached(max_results)
                                return
                    else:
                        score = self._score_match(
                            entry.file_name,
                            match_plan,
                            allow_partial_fuzzy=False,
                        )
                        if score is not None:
                            result = SearchResult(
                                entry.file_path,
                                mod_time=entry.mod_time,
                                file_size=entry.file_size,
                                match_score=score,
                            )
                            if deferred_results is not None:
                                deferred_results.append(result)
                            else:
                                yield result
                                emitted_results += 1
                                if max_results is not None and emitted_results >= max_results:
                                    if on_limit_reached:
                                        on_limit_reached(max_results)
                                    return
                except FileAccessError as e:
                    self.logger.warning(f"Skipping file due to access error: {e}")
                    continue

                files_processed += 1
                self._update_cached_inventory_progress(
                    files_processed,
                    len(inventory.files),
                    progress_callback,
                )

        except PermissionError as e:
            raise DirectoryError(f"Permission denied accessing directory: {directory}") from e
        except FileNotFoundError as e:
            raise DirectoryError(f"Directory not found: {directory}") from e
        except Exception as e:
            self.logger.exception("Unexpected error during search")
            raise SearchError(f"Search operation failed: {e!s}") from e

        if deferred_results:
            deferred_results.sort(
                key=lambda result: (result.match_score or 0.0, result.file_path.lower()),
                reverse=True,
            )
            if max_results is not None and len(deferred_results) > max_results:
                for result in deferred_results[:max_results]:
                    yield result
                    emitted_results += 1
                if on_limit_reached:
                    on_limit_reached(max_results)
                return
            for result in deferred_results:
                yield result
                emitted_results += 1

    def _validate_search_params(self, directory: str, search_term: str):
        """Validate search parameters."""
        if not directory or not directory.strip():
            raise ValidationError("Directory path cannot be empty")

        if not os.path.exists(directory):
            raise DirectoryError(f"Directory does not exist: {directory}")

        if not os.path.isdir(directory):
            raise DirectoryError(f"Path is not a directory: {directory}")

        if not search_term or not search_term.strip():
            raise ValidationError("Search term cannot be empty")

    def _normalize_file_types(self, file_types: list[str] | None) -> list[str]:
        """Normalize file type filters to lower-case extensions."""
        normalized: list[str] = []
        for file_type in file_types or []:
            value = file_type.strip().lower()
            if not value:
                continue
            if not value.startswith(".") and not any(char in value for char in "*?[]"):
                value = f".{value}"
            normalized.append(value)
        return normalized

    def _get_inventory_snapshot(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        progress_callback,
        cancel_event: threading.Event | None,
    ) -> InventorySnapshot | None:
        """Return a fresh or cached inventory snapshot for scan-based searches."""
        cache_key = InventoryCacheKey(
            directory=os.path.realpath(directory),
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
        )
        root_mtime_ns = self._get_directory_mtime_ns(directory)
        stale_snapshot: InventorySnapshot | None = None
        with self._inventory_cache_lock:
            snapshot = self._inventory_cache.get(cache_key)
            if snapshot is not None and self._is_inventory_snapshot_fresh(
                snapshot,
                root_mtime_ns=root_mtime_ns,
            ):
                if progress_callback:
                    progress_callback(
                        f"Reusing cached file inventory ({len(snapshot.files)} files)"
                    )
                return snapshot
            stale_snapshot = snapshot

        load_result = self._index_store.load_snapshot(
            cache_key,
            root_mtime_ns=root_mtime_ns,
            max_age_s=PERSISTENT_INDEX_MAX_AGE_S,
            allow_stale=True,
        )
        if load_result is not None:
            snapshot = load_result.snapshot
            with self._inventory_cache_lock:
                self._inventory_cache[cache_key] = snapshot
            if load_result.is_fresh:
                if progress_callback:
                    progress_callback(
                        f"Loaded persistent file inventory ({len(snapshot.files)} files)"
                    )
                return snapshot
            stale_snapshot = snapshot

        if stale_snapshot is not None:
            if progress_callback:
                progress_callback(self.BACKGROUND_REFRESH_STATUS)
            self._schedule_inventory_refresh(
                cache_key=cache_key,
                directory=directory,
                max_depth=max_depth,
                follow_symlinks=follow_symlinks,
            )
            return stale_snapshot

        snapshot = self._build_inventory_snapshot(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
            root_mtime_ns=root_mtime_ns,
        )
        if snapshot is None:
            return None

        self._store_inventory_snapshot(cache_key, snapshot)
        return snapshot

    def _get_directory_mtime_ns(self, directory: str) -> int | None:
        """Read the root directory mtime for cache freshness checks."""
        try:
            return os.stat(directory).st_mtime_ns
        except OSError:
            return None

    def _is_inventory_snapshot_fresh(
        self,
        snapshot: InventorySnapshot,
        *,
        root_mtime_ns: int | None,
    ) -> bool:
        """Check whether a cached snapshot can be reused safely enough."""
        if time() - snapshot.created_at > INVENTORY_CACHE_TTL_S:
            return False
        return snapshot.root_mtime_ns == root_mtime_ns

    def _store_inventory_snapshot(
        self,
        cache_key: InventoryCacheKey,
        snapshot: InventorySnapshot,
    ) -> None:
        """Persist an inventory snapshot and update the hot in-memory cache."""
        with self._inventory_cache_lock:
            self._inventory_cache[cache_key] = snapshot
            if len(self._inventory_cache) > INVENTORY_CACHE_MAX_ENTRIES:
                oldest_key = min(
                    self._inventory_cache,
                    key=lambda key: self._inventory_cache[key].created_at,
                )
                self._inventory_cache.pop(oldest_key, None)
        self._index_store.save_snapshot(cache_key, snapshot)

    def _schedule_inventory_refresh(
        self,
        *,
        cache_key: InventoryCacheKey,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
    ) -> None:
        """Kick off a deduplicated background inventory refresh."""
        with self._inventory_refresh_lock:
            if cache_key in self._inventory_refreshes:
                return
            self._inventory_refreshes.add(cache_key)

        refresh_thread = threading.Thread(
            target=self._refresh_inventory_in_background,
            kwargs={
                "cache_key": cache_key,
                "directory": directory,
                "max_depth": max_depth,
                "follow_symlinks": follow_symlinks,
            },
            daemon=True,
        )
        refresh_thread.start()

    def _refresh_inventory_in_background(
        self,
        *,
        cache_key: InventoryCacheKey,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
    ) -> None:
        """Rebuild a stale inventory snapshot without blocking the active search."""
        try:
            self.logger.info(f"Refreshing persistent inventory index for {directory}")
            snapshot = self._build_inventory_snapshot(
                directory=directory,
                max_depth=max_depth,
                follow_symlinks=follow_symlinks,
                progress_callback=None,
                cancel_event=None,
                root_mtime_ns=self._get_directory_mtime_ns(directory),
            )
            if snapshot is None:
                return
            self._store_inventory_snapshot(cache_key, snapshot)
            self.logger.info(f"Finished refreshing persistent inventory index for {directory}")
        except Exception as exc:
            self.logger.warning(f"Background inventory refresh failed for {directory}: {exc}")
        finally:
            with self._inventory_refresh_lock:
                self._inventory_refreshes.discard(cache_key)

    def _build_inventory_snapshot(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        progress_callback,
        cancel_event: threading.Event | None,
        root_mtime_ns: int | None,
    ) -> InventorySnapshot | None:
        """Build a file inventory for repeated scan-based searches."""
        if progress_callback:
            progress_callback(f"Building file inventory: {directory}")

        files: list[InventoryEntry] = []
        directories: list[str] = []
        files_indexed = 0
        for dir_path, entry in self._walk_scandir(
            directory,
            max_depth,
            follow_symlinks,
            cancel_event,
        ):
            if cancel_event and cancel_event.is_set():
                return None
            if entry is None:
                directories.append(dir_path)
                continue

            try:
                entry_stat = entry.stat(follow_symlinks=follow_symlinks)
            except OSError:
                continue

            files.append(
                InventoryEntry(
                    file_path=entry.path,
                    parent_dir=dir_path,
                    file_name=entry.name,
                    file_lower=entry.name.lower(),
                    mod_time=entry_stat.st_mtime,
                    file_size=entry_stat.st_size,
                )
            )
            files_indexed += 1
            if progress_callback and files_indexed % INVENTORY_PROGRESS_MILESTONE == 0:
                progress_callback(f"Indexed {files_indexed} files in {directory}")

        return InventorySnapshot(
            files=files,
            directories=directories,
            created_at=time(),
            root_mtime_ns=root_mtime_ns,
        )

    def _check_cached_inventory_filters(
        self,
        entry: InventoryEntry,
        *,
        include_types: list[str],
        exclude_types: list[str],
        search_within_files: bool,
        min_size: int | None,
        max_size: int | None,
        modified_after_ts: float | None,
        modified_before_ts: float | None,
    ) -> bool:
        """Apply query filters against cached inventory metadata."""
        if not self._check_file_filters(
            entry.file_size,
            entry.mod_time,
            min_size=min_size,
            max_size=max_size,
            modified_after_ts=modified_after_ts,
            modified_before_ts=modified_before_ts,
        ):
            return False
        return self._should_process_file(
            entry.file_lower,
            include_types,
            exclude_types,
            search_within_files=search_within_files,
            file_path=Path(entry.file_path),
        )

    def _build_match_plan(self, search_term: str, search_mode: SearchMode) -> MatchPlan:
        """Compile reusable match state for the active query."""
        normalized_term = search_term.lower()
        regex: re.Pattern[str] | None = None
        if search_mode == SearchMode.REGEX:
            try:
                regex = re.compile(search_term, re.IGNORECASE)
            except re.error as exc:
                raise ValidationError(f"Invalid regular expression: {exc}") from exc
        return MatchPlan(
            mode=search_mode,
            raw_term=search_term,
            normalized_term=normalized_term,
            regex=regex,
        )

    def _is_likely_binary(self, file_path: Path) -> bool:
        """Return True if the file appears to be binary (contains null bytes in the first 8 KB)."""
        try:
            with open(file_path, "rb") as f:
                return b"\x00" in f.read(8192)
        except OSError:
            return True

    def _should_process_file(
        self,
        file_lower: str,
        include_types: list[str],
        exclude_types: list[str],
        search_within_files: bool = True,
        file_path: Path | None = None,
    ) -> bool:
        """Check if file should be processed based on type filters."""
        # Skip known binary file types for content search only; allow through if
        # the file doesn't actually contain null bytes (e.g. text-based USD/USDA).
        file_ext = Path(file_lower).suffix.lower()
        if (
            search_within_files
            and file_ext in self._binary_extensions
            and (file_path is None or self._is_likely_binary(file_path))
        ):
            return False

        if include_types and not any(file_lower.endswith(ext) for ext in include_types):
            return False
        return not (exclude_types and any(file_lower.endswith(ext) for ext in exclude_types))

    def _resolve_search_backend(
        self,
        search_within_files: bool,
        search_mode: SearchMode,
        requested_backend: SearchBackend,
    ) -> SearchBackend:
        """Select the concrete backend while preserving Python fallback semantics."""
        if not search_within_files or search_mode == SearchMode.FUZZY:
            return SearchBackend.PYTHON
        if requested_backend == SearchBackend.PYTHON:
            return SearchBackend.PYTHON
        if self._rg_path is None:
            return SearchBackend.PYTHON
        return SearchBackend.RIPGREP

    def _score_match(
        self,
        text: str,
        match_plan: MatchPlan,
        *,
        allow_partial_fuzzy: bool,
    ) -> float | None:
        """Return a match score, or None when the text does not match."""
        if match_plan.mode == SearchMode.SUBSTRING:
            return 100.0 if match_plan.normalized_term in text.lower() else None
        if match_plan.mode == SearchMode.GLOB:
            has_glob_chars = any(c in match_plan.raw_term for c in "*?[]")
            pattern = match_plan.raw_term if has_glob_chars else f"*{match_plan.raw_term}*"
            return 100.0 if fnmatch.fnmatch(text.lower(), pattern.lower()) else None
        if match_plan.mode == SearchMode.REGEX:
            assert match_plan.regex is not None
            return 100.0 if match_plan.regex.search(text) else None
        if match_plan.mode == SearchMode.FUZZY:
            return self._score_fuzzy_match(
                text, match_plan.normalized_term, allow_partial_fuzzy=allow_partial_fuzzy
            )
        return None

    def _matches_term(
        self, text: str, match_plan: MatchPlan, *, allow_partial_fuzzy: bool = True
    ) -> bool:
        """Return True when the text matches the compiled plan."""
        return (
            self._score_match(
                text,
                match_plan,
                allow_partial_fuzzy=allow_partial_fuzzy,
            )
            is not None
        )

    def _score_fuzzy_match(
        self, text: str, normalized_term: str, *, allow_partial_fuzzy: bool
    ) -> float | None:
        """Compute a fuzzy score with tighter thresholds for filename matching."""
        if not RAPIDFUZZ_AVAILABLE or not normalized_term:
            return None

        normalized_text = text.lower()
        if normalized_text == normalized_term:
            return 120.0

        wratio = float(fuzz.WRatio(normalized_term, normalized_text))  # pyright: ignore[reportPossiblyUnboundVariable]
        score = wratio
        if allow_partial_fuzzy:
            partial = float(
                fuzz.partial_ratio(normalized_term, normalized_text)  # pyright: ignore[reportPossiblyUnboundVariable]
            )
            score = max(score, partial * 0.9)
            threshold = FUZZY_PARTIAL_THRESHOLD
        else:
            simple = float(fuzz.ratio(normalized_term, normalized_text))  # pyright: ignore[reportPossiblyUnboundVariable]
            partial = float(
                fuzz.partial_ratio(normalized_term, normalized_text)  # pyright: ignore[reportPossiblyUnboundVariable]
            )
            score = max(score, simple, partial * 0.85)
            threshold = FUZZY_FULL_THRESHOLD
            if normalized_text.startswith(normalized_term):
                score += FUZZY_EXACT_BONUS
            elif normalized_term in normalized_text:
                score += FUZZY_WORD_BONUS

        return score if score >= threshold else None

    def _check_file_filters_with_stat(
        self,
        stat_result: os.stat_result,
        min_size: int | None,
        max_size: int | None,
        modified_after_ts: float | None,
        modified_before_ts: float | None,
    ) -> bool:
        """Return True if the file passes all size/date filters (using pre-fetched stat)."""
        return self._check_file_filters(
            stat_result.st_size,
            stat_result.st_mtime,
            min_size=min_size,
            max_size=max_size,
            modified_after_ts=modified_after_ts,
            modified_before_ts=modified_before_ts,
        )

    def _check_file_filters(
        self,
        file_size: int,
        mod_time: float,
        *,
        min_size: int | None,
        max_size: int | None,
        modified_after_ts: float | None,
        modified_before_ts: float | None,
    ) -> bool:
        """Return True if metadata passes all size/date filters."""
        if min_size is not None and file_size < min_size:
            return False
        if max_size is not None and file_size > max_size:
            return False
        if modified_after_ts is not None and mod_time < modified_after_ts:
            return False
        return modified_before_ts is None or mod_time <= modified_before_ts

    def _detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet if available, otherwise fall back to utf-8."""
        try:
            with open(file_path, "rb") as f:
                raw = f.read(4096)

            if raw.startswith(codecs.BOM_UTF8):
                return "utf-8-sig"
            if raw.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
                return "utf-16"
            if raw.startswith((codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE)):
                return "utf-32"

            if self._chardet is not None:
                detected = self._chardet.detect(raw)
                if detected and detected.get("confidence", 0) > 0.5:
                    return detected["encoding"] or "utf-8"

            if raw:
                even_nulls = raw[::2].count(0)
                odd_nulls = raw[1::2].count(0)
                if even_nulls > len(raw[::2]) // 4 or odd_nulls > len(raw[1::2]) // 4:
                    return "utf-16"

            for encoding in ("utf-8", "utf-8-sig", "utf-16"):
                try:
                    raw.decode(encoding)
                    return encoding
                except UnicodeDecodeError:
                    continue
        except Exception:
            pass
        return "utf-8"

    def _search_file_content(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search within file content for the search term using optimized methods."""
        if file_size == 0:
            return
        if file_size > LARGE_FILE_MMAP_THRESHOLD:
            yield from self._search_large_file(file_path, match_plan, file_size=file_size)
        else:
            yield from self._search_small_file(file_path, match_plan, file_size=file_size)

    def _search_small_file(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Search small files using standard file reading."""
        try:
            encoding = self._detect_encoding(file_path)
            with open(file_path, encoding=encoding, errors="ignore") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                score = self._score_match(line, match_plan, allow_partial_fuzzy=True)
                if score is not None:
                    line_content = line.strip()
                    if len(line_content) > LINE_CONTENT_MAX_CHARS:
                        line_content = line_content[:LINE_CONTENT_MAX_CHARS] + "..."
                    next_line = lines[i + 1].strip() if i + 1 < len(lines) else None
                    if next_line and len(next_line) > LINE_CONTENT_MAX_CHARS:
                        next_line = next_line[:LINE_CONTENT_MAX_CHARS] + "..."
                    yield SearchResult(
                        str(file_path),
                        i + 1,
                        line_content,
                        next_line,
                        file_size=file_size,
                        match_score=score,
                    )
        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except OSError as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _search_file_content_with_ripgrep(
        self,
        directory: str,
        match_plan: MatchPlan,
        include_types: list[str],
        exclude_types: list[str],
        max_depth: int | None,
        min_size: int | None,
        max_size: int | None,
        max_results: int | None,
        modified_after_ts: float | None,
        modified_before_ts: float | None,
        follow_symlinks: bool,
        progress_callback=None,
        cancel_event: threading.Event | None = None,
        on_limit_reached: Callable[[int], None] | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Search file contents with ripgrep and filter matches using the app's metadata rules."""
        if self._rg_path is None:
            return

        command = self._build_ripgrep_command(
            directory=directory,
            match_plan=match_plan,
            include_types=include_types,
            exclude_types=exclude_types,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
        )
        if progress_callback:
            progress_callback(f"Searching with ripgrep: {directory}")

        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as e:
            self.logger.warning(f"ripgrep backend unavailable, falling back to Python search: {e}")
            yield from self.search_files(
                directory=directory,
                search_term=match_plan.raw_term,
                include_types=include_types,
                exclude_types=exclude_types,
                search_within_files=True,
                search_mode=match_plan.mode,
                search_backend=SearchBackend.PYTHON,
                max_depth=max_depth,
                min_size=min_size,
                max_size=max_size,
                modified_after=(
                    datetime.fromtimestamp(modified_after_ts)
                    if modified_after_ts is not None
                    else None
                ),
                modified_before=(
                    datetime.fromtimestamp(modified_before_ts)
                    if modified_before_ts is not None
                    else None
                ),
                follow_symlinks=follow_symlinks,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
                on_limit_reached=on_limit_reached,
            )
            return

        assert process.stdout is not None
        stdout = process.stdout
        stdout_queue: Queue[str | None] = Queue()

        def _reader() -> None:
            for raw_line in stdout:
                stdout_queue.put(raw_line)
            stdout_queue.put(None)

        threading.Thread(target=_reader, daemon=True).start()
        search_root = Path(directory)
        stat_cache: dict[str, os.stat_result] = {}
        matches = 0
        emitted_results = 0

        try:
            while True:
                if cancel_event and cancel_event.is_set():
                    self._terminate_process(process)
                    return

                try:
                    raw_line = stdout_queue.get(timeout=0.1)
                except Empty:
                    if process.poll() is not None:
                        break
                    continue

                if raw_line is None:
                    break

                try:
                    payload = json.loads(raw_line)
                except json.JSONDecodeError:
                    self.logger.debug("Skipping non-JSON ripgrep output line: %r", raw_line)
                    continue

                if payload.get("type") != "match":
                    continue

                data = payload["data"]
                file_path = self._resolve_ripgrep_path(search_root, data["path"])
                cache_key = str(file_path)
                stat_result = stat_cache.get(cache_key)
                if stat_result is None:
                    try:
                        stat_result = file_path.stat(follow_symlinks=follow_symlinks)
                    except OSError:
                        continue
                    stat_cache[cache_key] = stat_result

                if not self._check_file_filters_with_stat(
                    stat_result, min_size, max_size, modified_after_ts, modified_before_ts
                ):
                    continue

                line_text = self._decode_ripgrep_text(data["lines"]).rstrip("\n").rstrip("\r")
                if len(line_text) > LINE_CONTENT_MAX_CHARS:
                    line_text = line_text[:LINE_CONTENT_MAX_CHARS] + "..."

                matches += 1
                if progress_callback and matches % RIPGREP_PROGRESS_MILESTONE == 0:
                    progress_callback(f"ripgrep matched {matches} lines in {directory}")

                yield SearchResult(
                    str(file_path),
                    data.get("line_number"),
                    line_text.strip(),
                    file_size=stat_result.st_size,
                    mod_time=stat_result.st_mtime,
                )
                emitted_results += 1
                if max_results is not None and emitted_results >= max_results:
                    if on_limit_reached:
                        on_limit_reached(max_results)
                    return

            return_code = process.wait()
            if return_code not in (0, 1):
                stderr = ""
                if process.stderr is not None:
                    stderr = process.stderr.read().strip()
                raise SearchError(stderr or f"ripgrep search failed with exit code {return_code}")
        finally:
            self._terminate_process(process)

    def _build_ripgrep_command(
        self,
        directory: str,
        match_plan: MatchPlan,
        include_types: list[str],
        exclude_types: list[str],
        max_depth: int | None,
        follow_symlinks: bool,
    ) -> list[str]:
        """Build a ripgrep command that preserves this app's file-selection semantics."""
        if self._rg_path is None:
            raise SearchError("ripgrep is not available")

        command = [
            self._rg_path,
            "--json",
            "--line-number",
            "--color",
            "never",
            "--no-messages",
            "--glob-case-insensitive",
            "--threads",
            str(self.max_workers),
            "-uu",
        ]
        if follow_symlinks:
            command.append("-L")
        if max_depth is not None:
            command.extend(["--max-depth", str(max_depth)])
        for ext in include_types:
            command.extend(["-g", f"*{ext}"])
        for ext in exclude_types:
            command.extend(["-g", f"!*{ext}"])

        pattern = match_plan.raw_term
        if match_plan.mode == SearchMode.SUBSTRING:
            command.append("--fixed-strings")
        elif match_plan.mode == SearchMode.GLOB:
            pattern = self._translate_glob_to_regex(match_plan.raw_term)

        command.extend(["-i", pattern, directory])
        return command

    def _translate_glob_to_regex(self, search_term: str) -> str:
        """Translate the app's line-glob semantics into a ripgrep-compatible regex."""
        pattern = search_term if any(c in search_term for c in "*?[]") else f"*{search_term}*"
        translated: list[str] = []
        idx = 0
        while idx < len(pattern):
            char = pattern[idx]
            if char == "*":
                translated.append(".*")
            elif char == "?":
                translated.append(".")
            elif char == "[":
                end = idx + 1
                while end < len(pattern) and pattern[end] != "]":
                    end += 1
                if end >= len(pattern):
                    translated.append(r"\[")
                else:
                    contents = pattern[idx + 1 : end]
                    negate = contents.startswith(("!", "^"))
                    if negate:
                        contents = contents[1:]
                    safe_contents: list[str] = []
                    for pos, content_char in enumerate(contents):
                        if content_char == "\\":
                            safe_contents.append(r"\\")
                        elif content_char == "]":
                            safe_contents.append(r"\]")
                        elif content_char == "^" and pos == 0:
                            safe_contents.append(r"\^")
                        else:
                            safe_contents.append(content_char)
                    prefix = "^" if negate else ""
                    translated.append(f"[{prefix}{''.join(safe_contents)}]")
                    idx = end
            else:
                translated.append(re.escape(char))
            idx += 1
        return f"^{''.join(translated)}$"

    def _resolve_ripgrep_path(self, search_root: Path, path_info: dict[str, str]) -> Path:
        """Resolve ripgrep's match path into an absolute path under the searched root."""
        raw_path = self._decode_ripgrep_text(path_info)
        path = Path(raw_path)
        if path.is_absolute():
            return path
        return search_root / path

    def _decode_ripgrep_text(self, payload: dict[str, str]) -> str:
        """Decode a ripgrep JSON text payload that may contain plain text or base64 bytes."""
        if "text" in payload:
            return payload["text"]
        if "bytes" in payload:
            return b64decode(payload["bytes"]).decode("utf-8", errors="replace")
        return ""

    def _terminate_process(self, process: subprocess.Popen[str]) -> None:
        """Terminate a subprocess if it is still running."""
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()

    def _search_large_file(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Search large files using memory mapping for better performance."""
        try:
            with open(file_path, "rb") as f:
                try:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        encoding = self._detect_encoding(file_path)
                        line_no = 0
                        pos = 0
                        while pos < len(mm):
                            eol = mm.find(b"\n", pos)
                            end = eol if eol != -1 else len(mm)
                            line_bytes = mm[pos:end]
                            line_text = line_bytes.decode(encoding, errors="replace").rstrip("\r")
                            line_no += 1
                            score = self._score_match(
                                line_text,
                                match_plan,
                                allow_partial_fuzzy=True,
                            )
                            if score is not None:
                                line_content = line_text.strip()
                                if len(line_content) > LINE_CONTENT_MAX_CHARS:
                                    line_content = line_content[:LINE_CONTENT_MAX_CHARS] + "..."
                                # Peek at next line for context_after
                                next_line: str | None = None
                                if eol != -1:
                                    next_eol = mm.find(b"\n", end + 1)
                                    next_end = next_eol if next_eol != -1 else len(mm)
                                    raw_next = (
                                        mm[end + 1 : next_end]
                                        .decode(encoding, errors="replace")
                                        .rstrip("\r")
                                        .strip()
                                    )
                                    if raw_next:
                                        if len(raw_next) > LINE_CONTENT_MAX_CHARS:
                                            raw_next = raw_next[:LINE_CONTENT_MAX_CHARS] + "..."
                                        next_line = raw_next
                                yield SearchResult(
                                    str(file_path),
                                    line_no,
                                    line_content,
                                    next_line,
                                    file_size=file_size,
                                    match_score=score,
                                )
                            pos = end + 1

                except (OSError, ValueError):
                    # Fallback to regular file reading if mmap fails
                    yield from self._search_small_file(file_path, match_plan, file_size=file_size)

        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except OSError as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _update_progress(self, files_processed: int, current_dir: str, progress_callback):
        """Update progress if callback provided."""
        if progress_callback and files_processed % 10 == 0:
            progress_callback(f"Scanning: {current_dir} ({files_processed} files)")

    def _update_cached_inventory_progress(
        self,
        files_processed: int,
        total_files: int,
        progress_callback,
    ) -> None:
        """Update progress while iterating an already-built inventory."""
        if progress_callback and files_processed % 100 == 0:
            progress_callback(f"Searching cached inventory: {files_processed}/{total_files} files")

    def _walk_scandir(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        cancel_event: threading.Event | None,
        _current_depth: int = 0,
        _seen_realpaths: set[str] | None = None,
    ) -> Generator[tuple[str, os.DirEntry[str] | None], None, None]:
        """Walk directory tree using os.scandir for cached d_type (no extra stat on Linux).

        Yields (dir_path, None) once per directory entered (for folder matching),
        then (dir_path, entry) for each file in that directory.
        """
        if max_depth is not None and _current_depth > max_depth:
            return
        if cancel_event and cancel_event.is_set():
            return
        if _seen_realpaths is None:
            _seen_realpaths = {os.path.realpath(directory)}

        # Yield directory notification (entry=None signals "entering directory")
        yield (directory, None)

        try:
            with os.scandir(directory) as entries:
                subdirs: list[os.DirEntry[str]] = []
                for entry in entries:
                    if cancel_event and cancel_event.is_set():
                        return
                    try:
                        if entry.is_file(follow_symlinks=follow_symlinks):
                            yield (directory, entry)
                        elif entry.is_dir(follow_symlinks=follow_symlinks):
                            subdirs.append(entry)
                    except OSError:
                        continue
        except (PermissionError, OSError):
            return

        for subdir in subdirs:
            subdir_path = subdir.path
            if follow_symlinks:
                real = os.path.realpath(subdir_path)
                if real in _seen_realpaths:
                    continue
                _seen_realpaths.add(real)
            yield from self._walk_scandir(
                subdir_path,
                max_depth,
                follow_symlinks,
                cancel_event,
                _current_depth + 1,
                _seen_realpaths,
            )
