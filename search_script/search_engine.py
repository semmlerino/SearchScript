import codecs
import concurrent.futures
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
from collections import deque
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from queue import Empty, Queue
from time import time

try:
    from rapidfuzz import fuzz

    _rapidfuzz_available: bool = True
except ImportError:
    _rapidfuzz_available = False

import pathspec

from .config import DirectoryError, FileAccessError, SearchError, ValidationError
from .constants import (
    ADAPTIVE_TTL_DIVISOR,
    ADAPTIVE_TTL_SCAN_THRESHOLD_S,
    CONTENT_SEARCH_POOL_CHUNK_SIZE,
    FUZZY_EXACT_BONUS,
    FUZZY_FULL_THRESHOLD,
    FUZZY_PARTIAL_THRESHOLD,
    FUZZY_WORD_BONUS,
    INVENTORY_CACHE_MAX_ENTRIES,
    INVENTORY_CACHE_TTL_S,
    INVENTORY_PROGRESS_MILESTONE,
    LARGE_FILE_MMAP_THRESHOLD,
    LINE_CONTENT_MAX_CHARS,
    PERSISTENT_INDEX_MAX_AGE_CEILING_S,
    PERSISTENT_INDEX_MAX_AGE_S,
    RIPGREP_PROGRESS_MILESTONE,
    SPOT_CHECK_SAMPLE_SIZE,
)
from .search_index import (
    InventoryCacheKey,
    InventoryEntry,
    InventorySnapshot,
    SearchIndexStore,
)

RAPIDFUZZ_AVAILABLE: bool = _rapidfuzz_available


@dataclass
class SearchResult:
    file_path: str
    line_number: int | None = None
    line_content: str | None = None
    next_line: str | None = None
    mod_time: float | None = None
    file_size: int | None = None
    match_score: float | None = None
    context_before: list[str] | None = None
    context_after: list[str] | None = None
    match_start: int | None = None
    match_length: int | None = None

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
    case_sensitive: bool = False


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
        self._shutdown_event = threading.Event()
        self._index_store = SearchIndexStore(self.logger, db_path=index_db_path)
        self._always_binary_extensions = {
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
            ".usdc",
            ".usdz",
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
        self._maybe_binary_extensions = {".usd"}
        try:
            import chardet  # pyright: ignore[reportMissingImports]

            self._chardet: types.ModuleType | None = chardet
        except ImportError:
            self._chardet = None

    def shutdown(self) -> None:
        """Signal background threads to stop."""
        self._shutdown_event.set()

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
        include_ignored: bool = True,
        context_lines: int = 0,
        case_sensitive: bool = False,
        progress_callback: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
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
        match_plan = self._build_match_plan(search_term, search_mode, case_sensitive=case_sensitive)
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
                include_ignored=include_ignored,
                context_lines=context_lines,
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
            include_ignored=include_ignored,
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
                    result_tuple = self._score_match(
                        folder_name,
                        match_plan,
                        allow_partial_fuzzy=False,
                    )
                    if result_tuple is None:
                        continue
                    score, _, _ = result_tuple
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

            if search_within_files:
                # Collect filtered entries first
                content_entries: list[InventoryEntry] = []
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
                    content_entries.append(entry)
                    files_processed += 1
                    self._update_cached_inventory_progress(
                        files_processed,
                        len(inventory.files),
                        progress_callback,
                    )

                # Process in parallel chunks
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers
                ) as executor:
                    for chunk_start in range(
                        0, len(content_entries), CONTENT_SEARCH_POOL_CHUNK_SIZE
                    ):
                        if cancel_event and cancel_event.is_set():
                            return
                        chunk = content_entries[
                            chunk_start : chunk_start + CONTENT_SEARCH_POOL_CHUNK_SIZE
                        ]

                        def _make_task(
                            fp: Path,
                            fs: int,
                            mp: MatchPlan = match_plan,
                            cl: int = context_lines,
                        ) -> list[SearchResult]:
                            return list(
                                self._search_file_content(fp, mp, file_size=fs, context_lines=cl)
                            )

                        future_to_entry = {
                            executor.submit(_make_task, Path(e.file_path), e.file_size): e
                            for e in chunk
                        }
                        for future in concurrent.futures.as_completed(future_to_entry):
                            if cancel_event and cancel_event.is_set():
                                return
                            entry = future_to_entry[future]
                            try:
                                results = future.result()
                            except FileAccessError as exc:
                                self.logger.warning(f"Skipping file due to access error: {exc}")
                                continue
                            for result in results:
                                result.mod_time = entry.mod_time
                                yield result
                                emitted_results += 1
                                if max_results is not None and emitted_results >= max_results:
                                    if on_limit_reached:
                                        on_limit_reached(max_results)
                                    return
            else:
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

                    try:
                        result_tuple = self._score_match(
                            entry.file_name,
                            match_plan,
                            allow_partial_fuzzy=False,
                        )
                        if result_tuple is not None:
                            score, _, _ = result_tuple
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
                if on_limit_reached:
                    on_limit_reached(max_results)
                return
            yield from deferred_results

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
        progress_callback: Callable[[str], None] | None,
        cancel_event: threading.Event | None,
        include_ignored: bool = True,
    ) -> InventorySnapshot | None:
        """Return a fresh or cached inventory snapshot for scan-based searches."""
        cache_key = InventoryCacheKey(
            directory=os.path.realpath(directory),
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
        )
        stale_snapshot: InventorySnapshot | None = None
        with self._inventory_cache_lock:
            snapshot = self._inventory_cache.get(cache_key)
            if snapshot is not None and self._is_inventory_snapshot_fresh(snapshot):
                if progress_callback:
                    progress_callback(
                        f"Reusing cached file inventory ({len(snapshot.files)} files)"
                    )
                return snapshot
            stale_snapshot = snapshot

        load_result = self._index_store.load_snapshot(
            cache_key,
            max_age_s=PERSISTENT_INDEX_MAX_AGE_CEILING_S,
            allow_stale=True,
        )
        if load_result is not None:
            snapshot = load_result.snapshot
            effective_ttl = self._compute_effective_ttl(snapshot)
            age = time() - snapshot.created_at
            with self._inventory_cache_lock:
                self._inventory_cache[cache_key] = snapshot
            if age <= effective_ttl:
                if progress_callback:
                    progress_callback(
                        f"Loaded persistent file inventory ({len(snapshot.files)} files)"
                    )
                return snapshot
            stale_snapshot = snapshot

        if stale_snapshot is not None:
            if self._spot_check_snapshot(stale_snapshot):
                stale_snapshot.created_at = time()
                self._store_inventory_snapshot(cache_key, stale_snapshot)
                if progress_callback:
                    progress_callback(
                        f"Reusing validated file inventory ({len(stale_snapshot.files)} files)"
                    )
                return stale_snapshot
            if progress_callback:
                progress_callback(self.BACKGROUND_REFRESH_STATUS)
            self._schedule_inventory_refresh(
                cache_key=cache_key,
                directory=directory,
                max_depth=max_depth,
                follow_symlinks=follow_symlinks,
                include_ignored=include_ignored,
            )
            return stale_snapshot

        snapshot = self._build_inventory_snapshot(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
            include_ignored=include_ignored,
        )
        if snapshot is None:
            return None

        self._store_inventory_snapshot(cache_key, snapshot)
        return snapshot

    def _is_inventory_snapshot_fresh(
        self,
        snapshot: InventorySnapshot,
    ) -> bool:
        """Check whether a cached snapshot can be reused safely enough."""
        return time() - snapshot.created_at <= INVENTORY_CACHE_TTL_S

    def _compute_effective_ttl(self, snapshot: InventorySnapshot) -> float:
        """Return an adaptive persistent TTL based on how long the scan took."""
        if snapshot.scan_duration_s < ADAPTIVE_TTL_SCAN_THRESHOLD_S:
            return float(PERSISTENT_INDEX_MAX_AGE_S)
        scaled = PERSISTENT_INDEX_MAX_AGE_S * max(
            1.0, snapshot.scan_duration_s / ADAPTIVE_TTL_DIVISOR
        )
        return min(scaled, float(PERSISTENT_INDEX_MAX_AGE_CEILING_S))

    def _spot_check_snapshot(self, snapshot: InventorySnapshot) -> bool:
        """Sample recently-modified files and verify mtime+size still match.

        Returns True if all sampled files pass (snapshot likely still valid).
        """
        sorted_files = sorted(snapshot.files, key=lambda e: e.mod_time, reverse=True)
        sample = sorted_files[:SPOT_CHECK_SAMPLE_SIZE]
        for entry in sample:
            try:
                st = os.stat(entry.file_path)
            except OSError:
                return False
            if st.st_mtime != entry.mod_time or st.st_size != entry.file_size:
                return False
        return True

    def clear_inventory_cache(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool,
    ) -> None:
        """Public API: evict a specific inventory from both caches."""
        cache_key = InventoryCacheKey(
            directory=os.path.realpath(directory),
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
        )
        with self._inventory_cache_lock:
            self._inventory_cache.pop(cache_key, None)
        self._index_store.delete_snapshot(cache_key)

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
        include_ignored: bool = True,
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
                "include_ignored": include_ignored,
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
        include_ignored: bool = True,
    ) -> None:
        """Rebuild a stale inventory snapshot without blocking the active search."""
        try:
            self.logger.info(f"Refreshing persistent inventory index for {directory}")
            snapshot = self._build_inventory_snapshot(
                directory=directory,
                max_depth=max_depth,
                follow_symlinks=follow_symlinks,
                progress_callback=None,
                cancel_event=self._shutdown_event,
                include_ignored=include_ignored,
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
        progress_callback: Callable[[str], None] | None,
        cancel_event: threading.Event | None,
        include_ignored: bool = True,
    ) -> InventorySnapshot | None:
        """Build a file inventory for repeated scan-based searches."""
        if progress_callback:
            progress_callback(f"Building file inventory: {directory}")

        scan_start = time()
        files: list[InventoryEntry] = []
        directories: list[str] = []
        files_indexed = 0
        for dir_path, entry in self._walk_scandir(
            directory,
            max_depth,
            follow_symlinks,
            cancel_event,
            include_ignored,
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
            scan_duration_s=time() - scan_start,
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

    def _build_match_plan(
        self, search_term: str, search_mode: SearchMode, case_sensitive: bool = False
    ) -> MatchPlan:
        """Compile reusable match state for the active query."""
        normalized_term = search_term if case_sensitive else search_term.lower()
        regex: re.Pattern[str] | None = None
        if search_mode == SearchMode.REGEX:
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                regex = re.compile(search_term, flags)
            except re.error as exc:
                raise ValidationError(f"Invalid regular expression: {exc}") from exc
        return MatchPlan(
            mode=search_mode,
            raw_term=search_term,
            normalized_term=normalized_term,
            regex=regex,
            case_sensitive=case_sensitive,
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
        file_ext = Path(file_lower).suffix.lower()
        if search_within_files and file_ext in self._always_binary_extensions:
            return False
        # Maybe-binary formats (e.g. .usd) can be text or binary — sniff before skipping.
        if (
            search_within_files
            and file_ext in self._maybe_binary_extensions
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
    ) -> tuple[float, int, int] | None:
        """Return (score, match_start, match_length), or None when no match."""
        if match_plan.mode == SearchMode.SUBSTRING:
            haystack = text if match_plan.case_sensitive else text.lower()
            idx = haystack.find(match_plan.normalized_term)
            if idx >= 0:
                return (100.0, idx, len(match_plan.normalized_term))
            return None
        if match_plan.mode == SearchMode.GLOB:
            pattern = self._ensure_glob_wildcard(match_plan.raw_term)
            if match_plan.case_sensitive:
                matched = fnmatch.fnmatch(text, pattern)
            else:
                matched = fnmatch.fnmatch(text.lower(), pattern.lower())
            if matched:
                return (100.0, 0, len(text))
            return None
        if match_plan.mode == SearchMode.REGEX:
            assert match_plan.regex is not None
            m = match_plan.regex.search(text)
            if m:
                return (100.0, m.start(), m.end() - m.start())
            return None
        if match_plan.mode == SearchMode.FUZZY:
            score = self._score_fuzzy_match(
                text, match_plan.normalized_term, allow_partial_fuzzy=allow_partial_fuzzy
            )
            if score is not None:
                return (score, 0, 0)  # No precise position for fuzzy
            return None
        return None

    def _score_fuzzy_match(
        self, text: str, normalized_term: str, *, allow_partial_fuzzy: bool
    ) -> float | None:
        """Compute a fuzzy score with tighter thresholds for filename matching."""
        if not _rapidfuzz_available or not normalized_term:
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

    @staticmethod
    def _truncate_line(text: str) -> str:
        if len(text) > LINE_CONTENT_MAX_CHARS:
            return text[:LINE_CONTENT_MAX_CHARS] + "..."
        return text

    @staticmethod
    def _ensure_glob_wildcard(term: str) -> str:
        """Wrap term in wildcards if it contains no explicit glob characters."""
        return term if any(c in term for c in "*?[]") else f"*{term}*"

    def _detect_bom(self, file_path: Path) -> str | None:
        """Read up to 4 bytes and return the encoding if a BOM is present, else None."""
        try:
            with open(file_path, "rb") as f:
                raw = f.read(4)
            if raw[:3] == codecs.BOM_UTF8:
                return "utf-8-sig"
            if raw[:4] in (codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE):
                return "utf-32"
            if raw[:2] in (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
                return "utf-16"
        except OSError:
            return None
        return None

    def _search_file_content(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int = 0,
        context_lines: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search within file content for the search term using optimized methods."""
        if file_size == 0:
            return
        if file_size > LARGE_FILE_MMAP_THRESHOLD:
            yield from self._search_large_file(
                file_path, match_plan, file_size=file_size, context_lines=context_lines
            )
        else:
            yield from self._search_small_file(
                file_path, match_plan, file_size=file_size, context_lines=context_lines
            )

    def _search_small_file(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int | None = None,
        context_lines: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search small files using standard file reading."""
        try:
            encoding = self._detect_bom(file_path) or "utf-8"
            with open(file_path, encoding=encoding, errors="replace") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                result_tuple = self._score_match(line, match_plan, allow_partial_fuzzy=True)
                if result_tuple is not None:
                    score, raw_match_start, match_length = result_tuple
                    line_content = self._truncate_line(line.strip())
                    next_line = lines[i + 1].strip() if i + 1 < len(lines) else None
                    if next_line:
                        next_line = self._truncate_line(next_line)

                    # Adjust match_start for stripped leading whitespace
                    strip_offset = len(line) - len(line.lstrip())
                    match_start = max(0, raw_match_start - strip_offset)

                    ctx_before: list[str] | None = None
                    ctx_after: list[str] | None = None
                    if context_lines > 0:
                        before_start = max(0, i - context_lines)
                        ctx_before = [ln.strip() for ln in lines[before_start:i]]
                        after_end = min(len(lines), i + 1 + context_lines)
                        ctx_after = [ln.strip() for ln in lines[i + 1 : after_end]]
                        for lst in (ctx_before, ctx_after):
                            for idx, line_text in enumerate(lst):
                                lst[idx] = self._truncate_line(line_text)
                        # Override next_line with first after-context line when available
                        if ctx_after:
                            next_line = ctx_after[0]

                    yield SearchResult(
                        str(file_path),
                        i + 1,
                        line_content,
                        next_line,
                        file_size=file_size,
                        match_score=score,
                        context_before=ctx_before,
                        context_after=ctx_after,
                        match_start=match_start,
                        match_length=match_length,
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
        include_ignored: bool = True,
        context_lines: int = 0,
        progress_callback: Callable[[str], None] | None = None,
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
            include_ignored=include_ignored,
            context_lines=context_lines,
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
                include_ignored=include_ignored,
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
        pending_context: list[str] = []

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

                msg_type = payload.get("type")
                if msg_type == "context":
                    ctx_data = payload.get("data", {})
                    ctx_text = self._decode_ripgrep_text(ctx_data.get("lines", {})).strip()
                    ctx_text = self._truncate_line(ctx_text)
                    pending_context.append(ctx_text)
                    continue
                if msg_type != "match":
                    pending_context.clear()
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

                if not self._check_file_filters(
                    stat_result.st_size,
                    stat_result.st_mtime,
                    min_size=min_size,
                    max_size=max_size,
                    modified_after_ts=modified_after_ts,
                    modified_before_ts=modified_before_ts,
                ):
                    continue

                raw_line_text = self._decode_ripgrep_text(data["lines"]).rstrip("\n").rstrip("\r")
                stripped_line = raw_line_text.strip()
                stripped_line = self._truncate_line(stripped_line)

                # Parse submatch positions for highlighting
                submatches = data.get("submatches", [])
                rg_match_start: int | None = None
                rg_match_length: int | None = None
                if submatches:
                    sm = submatches[0]
                    sm_start = sm.get("start")
                    sm_end = sm.get("end")
                    if sm_start is not None and sm_end is not None:
                        strip_offset = len(raw_line_text) - len(raw_line_text.lstrip())
                        rg_match_start = max(0, sm_start - strip_offset)
                        rg_match_length = sm_end - sm_start

                # Collect context lines from ripgrep output
                rg_ctx_before: list[str] | None = None
                rg_ctx_after: list[str] | None = None
                if context_lines > 0:
                    rg_ctx_before = list(pending_context) if pending_context else None
                    pending_context.clear()
                    # Lookahead: read context entries after the match
                    after_lines: list[str] = []
                    putback: list[str] = []
                    while len(after_lines) < context_lines:
                        try:
                            peek_line = stdout_queue.get(timeout=0.1)
                        except Empty:
                            if process.poll() is not None:
                                break
                            continue
                        if peek_line is None:
                            stdout_queue.put(None)
                            break
                        try:
                            peek_payload = json.loads(peek_line)
                        except json.JSONDecodeError:
                            continue
                        if peek_payload.get("type") == "context":
                            peek_data = peek_payload.get("data", {})
                            peek_text = self._decode_ripgrep_text(
                                peek_data.get("lines", {})
                            ).strip()
                            peek_text = self._truncate_line(peek_text)
                            after_lines.append(peek_text)
                        else:
                            putback.append(peek_line)
                            break
                    for pb in putback:
                        stdout_queue.put(pb)
                    rg_ctx_after = after_lines if after_lines else None
                else:
                    pending_context.clear()

                matches += 1
                if progress_callback and matches % RIPGREP_PROGRESS_MILESTONE == 0:
                    progress_callback(f"ripgrep matched {matches} lines in {directory}")

                yield SearchResult(
                    str(file_path),
                    data.get("line_number"),
                    stripped_line,
                    file_size=stat_result.st_size,
                    mod_time=stat_result.st_mtime,
                    context_before=rg_ctx_before,
                    context_after=rg_ctx_after,
                    match_start=rg_match_start,
                    match_length=rg_match_length,
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
        include_ignored: bool = True,
        context_lines: int = 0,
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
            "--threads",
            str(self.max_workers),
        ]
        if include_ignored:
            command.append("-uu")
        if follow_symlinks:
            command.append("-L")
        if max_depth is not None:
            command.extend(["--max-depth", str(max_depth)])
        if context_lines > 0:
            command.extend(["-C", str(context_lines)])
        for ext in include_types:
            command.extend(["-g", f"*{ext}"])
        for ext in exclude_types:
            command.extend(["-g", f"!*{ext}"])

        pattern = match_plan.raw_term
        if match_plan.mode == SearchMode.SUBSTRING:
            command.append("--fixed-strings")
        elif match_plan.mode == SearchMode.GLOB:
            pattern = self._translate_glob_to_regex(match_plan.raw_term)

        if not match_plan.case_sensitive:
            command.append("-i")
            command.append("--glob-case-insensitive")
        command.extend([pattern, directory])
        return command

    def _translate_glob_to_regex(self, search_term: str) -> str:
        """Translate the app's line-glob semantics into a ripgrep-compatible regex."""
        pattern = self._ensure_glob_wildcard(search_term)
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
        context_lines: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search large files using memory mapping for better performance."""
        try:
            with open(file_path, "rb") as f:
                try:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        encoding = self._detect_bom(file_path) or "utf-8"
                        line_no = 0
                        pos = 0
                        maxlen = context_lines if context_lines > 0 else 0
                        prev_lines: deque[str] = deque(maxlen=maxlen)
                        while pos < len(mm):
                            eol = mm.find(b"\n", pos)
                            end = eol if eol != -1 else len(mm)
                            line_bytes = mm[pos:end]
                            line_text = line_bytes.decode(encoding, errors="replace").rstrip("\r")
                            line_no += 1
                            result_tuple = self._score_match(
                                line_text,
                                match_plan,
                                allow_partial_fuzzy=True,
                            )
                            if result_tuple is not None:
                                score, raw_match_start, match_length = result_tuple
                                line_content = self._truncate_line(line_text.strip())
                                # Adjust match_start for stripped leading whitespace
                                strip_offset = len(line_text) - len(line_text.lstrip())
                                match_start = max(0, raw_match_start - strip_offset)
                                # Scan forward for next_line and context_after
                                after_lines: list[str] = []
                                scan_count = max(1, context_lines)
                                scan_pos = end + 1
                                if eol != -1:
                                    for _ in range(scan_count):
                                        if scan_pos >= len(mm):
                                            break
                                        next_eol = mm.find(b"\n", scan_pos)
                                        next_end = next_eol if next_eol != -1 else len(mm)
                                        raw_next = (
                                            mm[scan_pos:next_end]
                                            .decode(encoding, errors="replace")
                                            .rstrip("\r")
                                            .strip()
                                        )
                                        if raw_next:
                                            after_lines.append(self._truncate_line(raw_next))
                                        scan_pos = next_end + 1
                                next_line = after_lines[0] if after_lines else None
                                ctx_before: list[str] | None = None
                                ctx_after: list[str] | None = None
                                if context_lines > 0:
                                    ctx_before = list(prev_lines)
                                    ctx_after = after_lines if after_lines else None
                                yield SearchResult(
                                    str(file_path),
                                    line_no,
                                    line_content,
                                    next_line,
                                    file_size=file_size,
                                    match_score=score,
                                    context_before=ctx_before,
                                    context_after=ctx_after,
                                    match_start=match_start,
                                    match_length=match_length,
                                )
                            prev_lines.append(line_text.strip())
                            pos = end + 1

                except (OSError, ValueError):
                    # Fallback to regular file reading if mmap fails
                    yield from self._search_small_file(
                        file_path, match_plan, file_size=file_size, context_lines=context_lines
                    )

        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except OSError as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _update_cached_inventory_progress(
        self,
        files_processed: int,
        total_files: int,
        progress_callback: Callable[[str], None] | None,
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
        include_ignored: bool = True,
        _current_depth: int = 0,
        _seen_realpaths: set[str] | None = None,
        _gitignore_specs: "list[tuple[str, pathspec.PathSpec]] | None" = None,
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

        if not include_ignored:
            gitignore_path = os.path.join(directory, ".gitignore")
            if os.path.isfile(gitignore_path):
                try:
                    with open(gitignore_path) as f:
                        new_spec = pathspec.PathSpec.from_lines("gitignore", f)
                    _gitignore_specs = (_gitignore_specs or []) + [(directory, new_spec)]
                except OSError:
                    pass

        # Yield directory notification (entry=None signals "entering directory")
        yield (directory, None)

        try:
            with os.scandir(directory) as entries:
                subdirs: list[os.DirEntry[str]] = []
                for entry in entries:
                    if cancel_event and cancel_event.is_set():
                        return
                    try:
                        if _gitignore_specs is not None:
                            ignored = False
                            for spec_dir, spec in _gitignore_specs:
                                rel_path = entry.path[len(spec_dir) :].lstrip(os.sep)
                                if entry.is_dir(follow_symlinks=follow_symlinks):
                                    if spec.match_file(rel_path + "/"):
                                        ignored = True
                                        break
                                elif spec.match_file(rel_path):
                                    ignored = True
                                    break
                            if ignored:
                                continue
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
                include_ignored,
                _current_depth + 1,
                _seen_realpaths,
                _gitignore_specs,
            )
