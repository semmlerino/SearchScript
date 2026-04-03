import json
import logging
import os
import sqlite3
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from time import time


@dataclass(frozen=True)
class InventoryCacheKey:
    directory: str
    max_depth: int | None
    follow_symlinks: bool


@dataclass(frozen=True)
class InventoryEntry:
    file_path: str
    parent_dir: str
    file_name: str
    file_lower: str
    mod_time: float
    file_size: int


@dataclass
class InventorySnapshot:
    files: list[InventoryEntry]
    directories: list[str]
    created_at: float
    root_mtime_ns: int | None


@dataclass(frozen=True)
class InventoryLoadResult:
    snapshot: InventorySnapshot
    is_fresh: bool


class SearchIndexStore:
    """SQLite-backed persistent inventory snapshots for repeated search roots."""

    MAX_STORED_INVENTORIES = 12

    def __init__(
        self,
        logger: logging.Logger | None = None,
        db_path: str | Path | None = None,
    ) -> None:
        self.logger = logger or logging.getLogger(__name__)
        self.db_path = Path(db_path) if db_path is not None else self._default_db_path()
        self._lock = threading.Lock()
        self._initialized = False
        self._available = True

    def load_snapshot(
        self,
        cache_key: InventoryCacheKey,
        *,
        root_mtime_ns: int | None,
        max_age_s: float,
        allow_stale: bool = False,
    ) -> InventoryLoadResult | None:
        """Load a persisted snapshot, optionally returning stale data for background refresh."""
        if not self._ensure_schema():
            return None

        serialized_key = self._serialize_cache_key(cache_key)
        try:
            with self._lock, sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                metadata = conn.execute(
                    """
                    SELECT created_at, root_mtime_ns
                    FROM inventories
                    WHERE cache_key = ?
                    """,
                    (serialized_key,),
                ).fetchone()
                if metadata is None:
                    return None

                created_at = float(metadata["created_at"])
                stored_root_mtime_ns = metadata["root_mtime_ns"]
                is_fresh = True
                if time() - created_at > max_age_s:
                    is_fresh = False
                if (
                    root_mtime_ns is not None
                    and stored_root_mtime_ns is not None
                    and int(stored_root_mtime_ns) != root_mtime_ns
                ):
                    is_fresh = False
                if not is_fresh and not allow_stale:
                    return None

                directories = [
                    str(row["dir_path"])
                    for row in conn.execute(
                        """
                        SELECT dir_path
                        FROM inventory_dirs
                        WHERE cache_key = ?
                        ORDER BY dir_path
                        """,
                        (serialized_key,),
                    )
                ]
                files = [
                    InventoryEntry(
                        file_path=str(row["file_path"]),
                        parent_dir=str(row["parent_dir"]),
                        file_name=str(row["file_name"]),
                        file_lower=str(row["file_lower"]),
                        mod_time=float(row["mod_time"]),
                        file_size=int(row["file_size"]),
                    )
                    for row in conn.execute(
                        """
                        SELECT file_path, parent_dir, file_name, file_lower, mod_time, file_size
                        FROM inventory_entries
                        WHERE cache_key = ?
                        ORDER BY file_path
                        """,
                        (serialized_key,),
                    )
                ]
        except sqlite3.Error as exc:
            self.logger.warning(f"Failed to load persistent inventory index: {exc}")
            return None

        return InventoryLoadResult(
            snapshot=InventorySnapshot(
                files=files,
                directories=directories,
                created_at=created_at,
                root_mtime_ns=(
                    int(stored_root_mtime_ns) if stored_root_mtime_ns is not None else None
                ),
            ),
            is_fresh=is_fresh,
        )

    def save_snapshot(
        self,
        cache_key: InventoryCacheKey,
        snapshot: InventorySnapshot,
    ) -> None:
        """Persist an inventory snapshot for reuse across app sessions."""
        if not self._ensure_schema():
            return

        serialized_key = self._serialize_cache_key(cache_key)
        try:
            with self._lock, sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute(
                    """
                    INSERT INTO inventories (
                        cache_key,
                        directory,
                        max_depth,
                        follow_symlinks,
                        created_at,
                        root_mtime_ns
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(cache_key) DO UPDATE SET
                        directory = excluded.directory,
                        max_depth = excluded.max_depth,
                        follow_symlinks = excluded.follow_symlinks,
                        created_at = excluded.created_at,
                        root_mtime_ns = excluded.root_mtime_ns
                    """,
                    (
                        serialized_key,
                        cache_key.directory,
                        cache_key.max_depth,
                        int(cache_key.follow_symlinks),
                        snapshot.created_at,
                        snapshot.root_mtime_ns,
                    ),
                )
                conn.execute(
                    "DELETE FROM inventory_entries WHERE cache_key = ?",
                    (serialized_key,),
                )
                conn.execute(
                    "DELETE FROM inventory_dirs WHERE cache_key = ?",
                    (serialized_key,),
                )
                conn.executemany(
                    """
                    INSERT INTO inventory_dirs (cache_key, dir_path)
                    VALUES (?, ?)
                    """,
                    ((serialized_key, directory) for directory in snapshot.directories),
                )
                conn.executemany(
                    """
                    INSERT INTO inventory_entries (
                        cache_key,
                        file_path,
                        parent_dir,
                        file_name,
                        file_lower,
                        mod_time,
                        file_size
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        (
                            serialized_key,
                            entry.file_path,
                            entry.parent_dir,
                            entry.file_name,
                            entry.file_lower,
                            entry.mod_time,
                            entry.file_size,
                        )
                        for entry in snapshot.files
                    ),
                )
                self._prune_old_snapshots(conn)
        except sqlite3.Error as exc:
            self.logger.warning(f"Failed to save persistent inventory index: {exc}")

    def _default_db_path(self) -> Path:
        """Return a platform-appropriate cache location for the SQLite index."""
        if os.name == "nt":
            base_dir = Path(os.getenv("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
        elif sys.platform == "darwin":
            base_dir = Path.home() / "Library" / "Caches"
        else:
            base_dir = Path(os.getenv("XDG_CACHE_HOME", Path.home() / ".cache"))
        return base_dir / "SearchScript" / "inventory_index.sqlite3"

    def _serialize_cache_key(self, cache_key: InventoryCacheKey) -> str:
        """Serialize an inventory cache key into a stable string identifier."""
        return json.dumps(
            {
                "directory": cache_key.directory,
                "max_depth": cache_key.max_depth,
                "follow_symlinks": cache_key.follow_symlinks,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def _ensure_schema(self) -> bool:
        """Create the database schema on first use."""
        if not self._available:
            return False

        with self._lock:
            if self._initialized:
                return True
            try:
                self.db_path.parent.mkdir(parents=True, exist_ok=True)
                with sqlite3.connect(self.db_path) as conn:
                    conn.executescript(
                        """
                        CREATE TABLE IF NOT EXISTS inventories (
                            cache_key TEXT PRIMARY KEY,
                            directory TEXT NOT NULL,
                            max_depth INTEGER,
                            follow_symlinks INTEGER NOT NULL,
                            created_at REAL NOT NULL,
                            root_mtime_ns INTEGER
                        );

                        CREATE TABLE IF NOT EXISTS inventory_dirs (
                            cache_key TEXT NOT NULL,
                            dir_path TEXT NOT NULL,
                            PRIMARY KEY (cache_key, dir_path)
                        );

                        CREATE TABLE IF NOT EXISTS inventory_entries (
                            cache_key TEXT NOT NULL,
                            file_path TEXT NOT NULL,
                            parent_dir TEXT NOT NULL,
                            file_name TEXT NOT NULL,
                            file_lower TEXT NOT NULL,
                            mod_time REAL NOT NULL,
                            file_size INTEGER NOT NULL,
                            PRIMARY KEY (cache_key, file_path)
                        );

                        CREATE INDEX IF NOT EXISTS idx_inventory_entries_cache_key
                        ON inventory_entries (cache_key);
                        """
                    )
            except (OSError, sqlite3.Error) as exc:
                self.logger.warning(f"Persistent inventory index unavailable: {exc}")
                self._available = False
                return False

            self._initialized = True
            return True

    def _prune_old_snapshots(self, conn: sqlite3.Connection) -> None:
        """Keep the persistent index bounded to a reasonable number of roots."""
        rows = conn.execute(
            """
            SELECT cache_key
            FROM inventories
            ORDER BY created_at DESC
            """
        ).fetchall()
        stale_keys = [row[0] for row in rows[self.MAX_STORED_INVENTORIES :]]
        for cache_key in stale_keys:
            conn.execute("DELETE FROM inventories WHERE cache_key = ?", (cache_key,))
            conn.execute("DELETE FROM inventory_dirs WHERE cache_key = ?", (cache_key,))
            conn.execute("DELETE FROM inventory_entries WHERE cache_key = ?", (cache_key,))
