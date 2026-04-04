"""InventoryManager: caching, TTL, spot-check, background refresh, and filesystem walk."""

import contextlib
import heapq
import logging
import os
import queue
import threading
from collections.abc import Callable, Generator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from time import time

import pathspec

from .constants import (
    ADAPTIVE_TTL_DIVISOR,
    ADAPTIVE_TTL_SCAN_THRESHOLD_S,
    INVENTORY_CACHE_MAX_ENTRIES,
    INVENTORY_CACHE_TTL_S,
    INVENTORY_PROGRESS_MILESTONE,
    INVENTORY_WALK_MAX_WORKERS,
    NFS_INVENTORY_WALK_MAX_WORKERS,
    PERSISTENT_INDEX_MAX_AGE_CEILING_S,
    PERSISTENT_INDEX_MAX_AGE_S,
    PRUNED_DIRECTORY_NAMES,
    SPOT_CHECK_SAMPLE_SIZE,
)
from .file_utils import is_nfs_path
from .search_index import (
    InventoryCacheKey,
    InventoryEntry,
    InventorySnapshot,
    SearchIndexStore,
)


@dataclass
class _WalkWorkItem:
    directory: str
    depth: int
    gitignore_specs: list[tuple[str, pathspec.PathSpec]]


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

    def warm_snapshot(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool = True,
        exclude_shots: bool = True,
    ) -> None:
        """Warm a snapshot in the background for future Python-backed searches."""
        if self._shutdown_event.is_set():
            return
        cache_key = InventoryCacheKey(
            directory=os.path.realpath(directory),
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )
        with self._cache_lock:
            snapshot = self._cache.get(cache_key)
            if snapshot is not None and self._is_fresh(snapshot):
                return
        self._schedule_refresh(
            cache_key=cache_key,
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )

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
        scan_start = time()

        files, directories = self._walk_parallel(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            cancel_event=cancel_event,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
            progress_callback=progress_callback,
        )
        if cancel_event and cancel_event.is_set():
            return None

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
                            if entry.name in PRUNED_DIRECTORY_NAMES:
                                continue
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

    def _walk_parallel(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        cancel_event: threading.Event | None,
        include_ignored: bool = True,
        exclude_shots: bool = True,
        progress_callback: Callable[[str], None] | None = None,
    ) -> tuple[list[InventoryEntry], list[str]]:
        """Walk directory tree in parallel using a BFS thread-pool approach.

        Returns (files, directories) lists directly — not a generator.
        """
        if is_nfs_path(directory):
            max_workers = NFS_INVENTORY_WALK_MAX_WORKERS
        else:
            max_workers = min(INVENTORY_WALK_MAX_WORKERS, (os.cpu_count() or 4) * 2)

        all_files: list[InventoryEntry] = []
        all_dirs: list[str] = []
        files_lock = threading.Lock()
        seen_realpaths: set[str] = {os.path.realpath(directory)}
        seen_lock = threading.Lock()

        files_indexed = 0

        if progress_callback:
            progress_callback(f"Building file inventory: {directory}")

        results_queue: queue.Queue[tuple[list[InventoryEntry], list[str], list[_WalkWorkItem]]] = (
            queue.Queue()
        )

        pending_count = 0
        count_lock = threading.Lock()
        done_event = threading.Event()

        def _process_directory(
            work: _WalkWorkItem,
        ) -> tuple[list[InventoryEntry], list[str], list[_WalkWorkItem]]:
            local_files: list[InventoryEntry] = []
            local_dirs: list[str] = []
            sub_items: list[_WalkWorkItem] = []

            if cancel_event and cancel_event.is_set():
                return local_files, local_dirs, sub_items

            current_gitignore_specs = work.gitignore_specs

            if not include_ignored:
                gitignore_path = os.path.join(work.directory, ".gitignore")
                if os.path.isfile(gitignore_path):
                    try:
                        with open(gitignore_path) as f:
                            new_spec = pathspec.PathSpec.from_lines("gitignore", f)
                        current_gitignore_specs = [
                            *current_gitignore_specs,
                            (work.directory, new_spec),
                        ]
                    except OSError:
                        pass

            local_dirs.append(work.directory)

            try:
                with os.scandir(work.directory) as entries:
                    for entry in entries:
                        if cancel_event and cancel_event.is_set():
                            return local_files, local_dirs, sub_items
                        try:
                            if current_gitignore_specs:
                                ignored = False
                                for spec_dir, spec in current_gitignore_specs:
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
                                try:
                                    entry_stat = entry.stat(follow_symlinks=follow_symlinks)
                                except OSError:
                                    continue
                                local_files.append(
                                    InventoryEntry(
                                        file_path=entry.path,
                                        parent_dir=work.directory,
                                        file_name=entry.name,
                                        file_lower=entry.name.lower(),
                                        mod_time=entry_stat.st_mtime,
                                        file_size=entry_stat.st_size,
                                    )
                                )
                            elif entry.is_dir(follow_symlinks=follow_symlinks):
                                if entry.name in PRUNED_DIRECTORY_NAMES:
                                    continue
                                if exclude_shots and entry.name == "shots":
                                    continue
                                if max_depth is not None and work.depth + 1 > max_depth:
                                    continue
                                subdir_path = entry.path
                                if follow_symlinks:
                                    real = os.path.realpath(subdir_path)
                                    with seen_lock:
                                        if real in seen_realpaths:
                                            continue
                                        seen_realpaths.add(real)
                                sub_items.append(
                                    _WalkWorkItem(
                                        directory=subdir_path,
                                        depth=work.depth + 1,
                                        gitignore_specs=current_gitignore_specs,
                                    )
                                )
                        except OSError:
                            continue
            except (PermissionError, OSError):
                pass

            return local_files, local_dirs, sub_items

        def _on_done(
            fut: Future[tuple[list[InventoryEntry], list[str], list[_WalkWorkItem]]],
        ) -> None:
            with contextlib.suppress(Exception):  # OSError from scandir, etc.
                results_queue.put(fut.result())
            with count_lock:
                nonlocal pending_count
                pending_count -= 1
                if pending_count == 0:
                    done_event.set()

        initial_work = _WalkWorkItem(directory=directory, depth=0, gitignore_specs=[])

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with count_lock:
                pending_count = 1
            fut = executor.submit(_process_directory, initial_work)
            fut.add_done_callback(_on_done)

            while not done_event.is_set():
                if cancel_event and cancel_event.is_set():
                    break
                done_event.wait(timeout=0.1)
                # Drain results queue
                while not results_queue.empty():
                    try:
                        files, dirs, sub_items = results_queue.get_nowait()
                    except queue.Empty:
                        break
                    with files_lock:
                        all_files.extend(files)
                        all_dirs.extend(dirs)
                        files_indexed += len(files)
                    if progress_callback and files_indexed % INVENTORY_PROGRESS_MILESTONE == 0:
                        progress_callback(f"Indexed {files_indexed} files in {directory}")
                    for item in sub_items:
                        with count_lock:
                            pending_count += 1
                            done_event.clear()
                        f = executor.submit(_process_directory, item)
                        f.add_done_callback(_on_done)

            # Final drain after done_event is set
            while not results_queue.empty():
                try:
                    files, dirs, sub_items = results_queue.get_nowait()
                except queue.Empty:
                    break
                with files_lock:
                    all_files.extend(files)
                    all_dirs.extend(dirs)
                for item in sub_items:
                    with count_lock:
                        pending_count += 1
                        done_event.clear()
                    f = executor.submit(_process_directory, item)
                    f.add_done_callback(_on_done)

            # If we submitted more work in final drain, wait again
            if pending_count > 0:
                done_event.wait()
                while not results_queue.empty():
                    try:
                        files, dirs, _ = results_queue.get_nowait()
                    except queue.Empty:
                        break
                    with files_lock:
                        all_files.extend(files)
                        all_dirs.extend(dirs)

        return all_files, all_dirs
