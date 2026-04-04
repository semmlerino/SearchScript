"""InventoryManager: caching, TTL, spot-check, background refresh, and filesystem walk."""

import heapq
import logging
import os
import threading
from collections.abc import Callable, Generator
from dataclasses import replace
from pathlib import Path
from time import time

import pathspec

from .constants import (
    ADAPTIVE_TTL_DIVISOR,
    ADAPTIVE_TTL_SCAN_THRESHOLD_S,
    INVENTORY_CACHE_MAX_ENTRIES,
    INVENTORY_CACHE_TTL_S,
    INVENTORY_PROGRESS_MILESTONE,
    PERSISTENT_INDEX_MAX_AGE_CEILING_S,
    PERSISTENT_INDEX_MAX_AGE_S,
    SPOT_CHECK_SAMPLE_SIZE,
)
from .search_index import (
    InventoryCacheKey,
    InventoryEntry,
    InventorySnapshot,
    SearchIndexStore,
)


class InventoryManager:
    """Manages file inventory caching, TTL, spot-checking, and background refresh."""

    BACKGROUND_REFRESH_STATUS = "Using cached file inventory while refreshing index in background"

    def __init__(
        self,
        logger: logging.Logger,
        index_db_path: str | Path | None = None,
    ) -> None:
        self.logger = logger
        self._cache: dict[InventoryCacheKey, InventorySnapshot] = {}
        self._cache_lock = threading.Lock()
        self._refreshes: set[InventoryCacheKey] = set()
        self._refresh_lock = threading.Lock()
        self._shutdown_event = threading.Event()
        self._index_store = SearchIndexStore(logger, db_path=index_db_path)

    def get_snapshot(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        progress_callback: Callable[[str], None] | None,
        cancel_event: threading.Event | None,
        include_ignored: bool = True,
        exclude_shots: bool = True,
    ) -> InventorySnapshot | None:
        """Return a fresh or cached inventory snapshot for scan-based searches."""
        cache_key = InventoryCacheKey(
            directory=os.path.realpath(directory),
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )
        stale_snapshot: InventorySnapshot | None = None
        with self._cache_lock:
            snapshot = self._cache.get(cache_key)
            if snapshot is not None and self._is_fresh(snapshot):
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
            with self._cache_lock:
                self._cache[cache_key] = snapshot
            if age <= effective_ttl:
                if progress_callback:
                    progress_callback(
                        f"Loaded persistent file inventory ({len(snapshot.files)} files)"
                    )
                return snapshot
            stale_snapshot = snapshot

        if stale_snapshot is not None:
            if self._spot_check(stale_snapshot):
                refreshed = replace(stale_snapshot, created_at=time())
                self._store(cache_key, refreshed)
                if progress_callback:
                    progress_callback(
                        f"Reusing validated file inventory ({len(refreshed.files)} files)"
                    )
                return refreshed
            if progress_callback:
                progress_callback(self.BACKGROUND_REFRESH_STATUS)
            self._schedule_refresh(
                cache_key=cache_key,
                directory=directory,
                max_depth=max_depth,
                follow_symlinks=follow_symlinks,
                include_ignored=include_ignored,
                exclude_shots=exclude_shots,
            )
            return stale_snapshot

        snapshot = self._build_snapshot(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )
        if snapshot is None:
            return None

        self._store(cache_key, snapshot)
        return snapshot

    def clear_cache(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool,
        exclude_shots: bool = True,
    ) -> None:
        """Evict a specific inventory from both caches."""
        cache_key = InventoryCacheKey(
            directory=os.path.realpath(directory),
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )
        with self._cache_lock:
            self._cache.pop(cache_key, None)
        self._index_store.delete_snapshot(cache_key)

    def shutdown(self) -> None:
        """Signal background threads to stop."""
        self._shutdown_event.set()

    def _is_fresh(self, snapshot: InventorySnapshot) -> bool:
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

    def _spot_check(self, snapshot: InventorySnapshot) -> bool:
        """Sample recently-modified files and verify mtime+size still match.

        Returns True if all sampled files pass (snapshot likely still valid).
        """
        sample = heapq.nlargest(SPOT_CHECK_SAMPLE_SIZE, snapshot.files, key=lambda e: e.mod_time)
        for entry in sample:
            try:
                st = os.stat(entry.file_path)
            except OSError:
                return False
            if st.st_mtime != entry.mod_time or st.st_size != entry.file_size:
                return False
        return True

    def _store(
        self,
        cache_key: InventoryCacheKey,
        snapshot: InventorySnapshot,
    ) -> None:
        """Persist an inventory snapshot and update the hot in-memory cache."""
        with self._cache_lock:
            self._cache[cache_key] = snapshot
            if len(self._cache) > INVENTORY_CACHE_MAX_ENTRIES:
                oldest_key = min(
                    self._cache,
                    key=lambda key: self._cache[key].created_at,
                )
                self._cache.pop(oldest_key, None)
        self._index_store.save_snapshot(cache_key, snapshot)

    def _schedule_refresh(
        self,
        *,
        cache_key: InventoryCacheKey,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool = True,
        exclude_shots: bool = True,
    ) -> None:
        """Kick off a deduplicated background inventory refresh."""
        with self._refresh_lock:
            if cache_key in self._refreshes:
                return
            self._refreshes.add(cache_key)

        refresh_thread = threading.Thread(
            target=self._refresh_in_background,
            kwargs={
                "cache_key": cache_key,
                "directory": directory,
                "max_depth": max_depth,
                "follow_symlinks": follow_symlinks,
                "include_ignored": include_ignored,
                "exclude_shots": exclude_shots,
            },
            daemon=True,
        )
        refresh_thread.start()

    def _refresh_in_background(
        self,
        *,
        cache_key: InventoryCacheKey,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool = True,
        exclude_shots: bool = True,
    ) -> None:
        """Rebuild a stale inventory snapshot without blocking the active search."""
        try:
            self.logger.info(f"Refreshing persistent inventory index for {directory}")
            snapshot = self._build_snapshot(
                directory=directory,
                max_depth=max_depth,
                follow_symlinks=follow_symlinks,
                progress_callback=None,
                cancel_event=self._shutdown_event,
                include_ignored=include_ignored,
                exclude_shots=exclude_shots,
            )
            if snapshot is None:
                return
            self._store(cache_key, snapshot)
            self.logger.info(f"Finished refreshing persistent inventory index for {directory}")
        except Exception as exc:
            self.logger.warning(f"Background inventory refresh failed for {directory}: {exc}")
        finally:
            with self._refresh_lock:
                self._refreshes.discard(cache_key)

    def _build_snapshot(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        progress_callback: Callable[[str], None] | None,
        cancel_event: threading.Event | None,
        include_ignored: bool = True,
        exclude_shots: bool = True,
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
            exclude_shots=exclude_shots,
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

    def _walk_scandir(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        cancel_event: threading.Event | None,
        include_ignored: bool = True,
        exclude_shots: bool = True,
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
                            if exclude_shots and entry.name == "shots":
                                continue
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
                exclude_shots,
                _current_depth + 1,
                _seen_realpaths,
                _gitignore_specs,
            )
