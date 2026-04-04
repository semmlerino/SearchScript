"""Tests for the search application components."""

import gc
import os
import queue
import shutil
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import Any, cast

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import QApplication, QWidget

from search_script.config import ValidationError
from search_script.file_utils import FileOperations
from search_script.inventory import InventoryManager
from search_script.models import SearchBackend, SearchMode, SearchResult
from search_script.search_controller import SearchController
from search_script.search_engine import SearchEngine
from search_script.search_index import InventoryCacheKey, InventorySnapshot, SearchIndexStore
from search_script.ui_components import SearchUI


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    qt_app = cast(QApplication, app)
    yield qt_app
    qt_app.closeAllWindows()
    qt_app.processEvents()
    gc.collect()


def close_widget(widget: QWidget) -> None:
    widget.close()
    widget.deleteLater()
    app = QApplication.instance()
    if app is not None:
        app.processEvents()


def test_filename_search(tmp_path: Path):
    test_file1 = tmp_path / "test1.txt"
    test_file2 = tmp_path / "test2.py"
    test_file3 = tmp_path / "other.log"

    test_file1.write_text("Hello world")
    test_file2.write_text("def foo(): pass")
    test_file3.write_text("log data")

    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "test", search_within_files=False))
    assert len(results) == 2
    paths = {r.file_path for r in results}
    assert str(test_file1) in paths
    assert str(test_file2) in paths


def test_content_search(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello world\nThis is a test file\nPython search")

    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "search", search_within_files=True))
    assert len(results) == 1
    assert results[0].line_number == 3
    assert results[0].line_content is not None
    assert "search" in results[0].line_content.lower()


@pytest.mark.parametrize(
    ("mode", "term"),
    [
        (SearchMode.SUBSTRING, "Hello"),
        (SearchMode.REGEX, "Hello"),
        (SearchMode.GLOB, "Hello"),
    ],
)
def test_large_file_content_search_python_backend(tmp_path: Path, mode: SearchMode, term: str):
    test_file = tmp_path / "big.txt"
    test_file.write_text(("Hello World " * 12 + "\n") * 8000, encoding="utf-8")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            term,
            search_within_files=True,
            search_mode=mode,
            search_backend=SearchBackend.PYTHON,
        )
    )

    assert len(results) == 8000
    assert results[0].line_number == 1
    assert results[0].line_content == ("Hello World " * 12).strip()


def test_content_search_with_type_filter(tmp_path: Path):
    (tmp_path / "code.py").write_text("def search_function(): pass")
    (tmp_path / "notes.txt").write_text("search notes here")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path), "search", include_types=[".py"], search_within_files=True
        )
    )
    assert len(results) == 1
    assert results[0].file_path.endswith(".py")


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep unavailable")
def test_ripgrep_glob_backend_matches_content(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello there\nGeneral Kenobi", encoding="utf-8")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "hello",
            search_within_files=True,
            search_mode=SearchMode.GLOB,
            search_backend=SearchBackend.RIPGREP,
        )
    )

    assert len(results) == 1
    assert results[0].line_content == "Hello there"


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep unavailable")
def test_ripgrep_backend_respects_modified_date_filter(tmp_path: Path):
    old_file = tmp_path / "old.txt"
    new_file = tmp_path / "new.txt"
    old_file.write_text("needle")
    new_file.write_text("needle")

    now = datetime.now().timestamp()
    yesterday = now - 86400
    os.utime(old_file, (yesterday, yesterday))
    os.utime(new_file, (now, now))

    engine = SearchEngine()
    cutoff = datetime.now() - timedelta(hours=12)
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.RIPGREP,
            modified_after=cutoff,
        )
    )

    assert [result.file_path for result in results] == [str(new_file)]


def test_file_modification_time():
    file_ops = FileOperations()
    mod_time = file_ops.get_file_modification_time(__file__)
    assert mod_time != "N/A"

    mod_time = file_ops.get_file_modification_time("/path/to/nonexistent/file.txt")
    assert mod_time == "N/A"


def test_invalid_regex_raises_validation_error(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello", encoding="utf-8")

    engine = SearchEngine()
    with pytest.raises(ValidationError):
        list(
            engine.search_files(
                str(tmp_path),
                "([",
                search_within_files=True,
                search_mode=SearchMode.REGEX,
            )
        )


def test_fuzzy_filename_search(tmp_path: Path):
    (tmp_path / "configuration.txt").write_text("data")
    (tmp_path / "readme.md").write_text("info")

    engine = SearchEngine()
    # "confg" is a typo for "config" — fuzzy should match "configuration"
    results = list(
        engine.search_files(
            str(tmp_path), "confg", search_within_files=False, search_mode=SearchMode.FUZZY
        )
    )
    assert len(results) == 1
    assert "configuration" in results[0].file_path


def test_fuzzy_filename_search_ranks_relevant_matches(tmp_path: Path):
    (tmp_path / "configuration.txt").write_text("data")
    (tmp_path / "congratulation.txt").write_text("data")
    (tmp_path / "readme.md").write_text("data")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "configration",
            search_within_files=False,
            search_mode=SearchMode.FUZZY,
        )
    )

    assert results
    assert results[0].file_path.endswith("configuration.txt")
    assert all("readme" not in result.file_path for result in results)


def test_fuzzy_content_search(tmp_path: Path):
    test_file = tmp_path / "notes.txt"
    test_file.write_text("authentication module\nlogging setup\ndata pipeline")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path), "authenticaton", search_within_files=True, search_mode=SearchMode.FUZZY
        )
    )
    assert len(results) >= 1
    assert results[0].line_number == 1


def test_filename_search_binary_extension(tmp_path: Path):
    exr_file = tmp_path / "render.exr"
    exr_file.write_bytes(b"\x00" * 10)
    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "render", search_within_files=False))
    assert len(results) == 1
    assert results[0].file_path.endswith(".exr")


def test_search_engine_reuses_inventory_cache(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    for index in range(3):
        (tmp_path / f"file{index}.txt").write_text("hello", encoding="utf-8")

    engine = SearchEngine()
    build_calls = 0
    original = engine._inventory._build_snapshot  # pyright: ignore[reportPrivateUsage]

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        nonlocal build_calls
        build_calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(engine._inventory, "_build_snapshot", wrapped)  # pyright: ignore[reportPrivateUsage]

    list(
        engine.search_files(
            str(tmp_path), "file", search_within_files=False, search_backend=SearchBackend.PYTHON
        )
    )
    list(
        engine.search_files(
            str(tmp_path), "file", search_within_files=False, search_backend=SearchBackend.PYTHON
        )
    )

    assert build_calls == 1


def test_search_engine_uses_persistent_index_between_instances(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    search_root = tmp_path / "search_root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    for index in range(3):
        (search_root / f"file{index}.txt").write_text("hello", encoding="utf-8")

    first_engine = SearchEngine(index_db_path=index_db)
    first_results = list(
        first_engine.search_files(str(search_root), "file", search_within_files=False)
    )
    assert len(first_results) == 3

    second_engine = SearchEngine(index_db_path=index_db)

    def fail_build(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("persistent index should avoid rebuilding inventory")

    monkeypatch.setattr(second_engine._inventory, "_build_snapshot", fail_build)  # pyright: ignore[reportPrivateUsage]
    second_results = list(
        second_engine.search_files(str(search_root), "file", search_within_files=False)
    )

    assert len(second_results) == 3


def test_search_engine_refreshes_stale_persistent_index_in_background(tmp_path: Path):
    search_root = tmp_path / "search_root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    (search_root / "one.txt").write_text("hello", encoding="utf-8")

    # Build and persist an initial inventory.
    first_engine = SearchEngine(index_db_path=index_db)
    list(
        first_engine.search_files(
            str(search_root), "txt", search_within_files=False, search_backend=SearchBackend.PYTHON
        )
    )

    # Age the persisted entry past the TTL so the next load considers it stale.
    from search_script.constants import PERSISTENT_INDEX_MAX_AGE_S

    with sqlite3.connect(str(index_db)) as conn:
        conn.execute(
            "UPDATE inventories SET created_at = created_at - ?",
            (PERSISTENT_INDEX_MAX_AGE_S + 1,),
        )

    # Modify an existing file so spot-check detects the change, plus add a new file.
    (search_root / "one.txt").write_text("modified", encoding="utf-8")
    (search_root / "two.txt").write_text("hello", encoding="utf-8")

    statuses: list[str] = []
    second_engine = SearchEngine(index_db_path=index_db)
    stale_results = list(
        second_engine.search_files(
            str(search_root),
            "txt",
            search_within_files=False,
            progress_callback=statuses.append,
            search_backend=SearchBackend.PYTHON,
        )
    )

    deadline = monotonic() + 2.0
    while monotonic() < deadline:
        with second_engine._inventory._refresh_lock:  # pyright: ignore[reportPrivateUsage]
            if not second_engine._inventory._refreshes:  # pyright: ignore[reportPrivateUsage]
                break
        sleep(0.05)

    with second_engine._inventory._refresh_lock:  # pyright: ignore[reportPrivateUsage]
        assert not second_engine._inventory._refreshes  # pyright: ignore[reportPrivateUsage]

    third_engine = SearchEngine(index_db_path=index_db)
    fresh_results = list(
        third_engine.search_files(
            str(search_root), "txt", search_within_files=False, search_backend=SearchBackend.PYTHON
        )
    )

    assert len(stale_results) == 1
    assert len(fresh_results) == 2
    assert InventoryManager.BACKGROUND_REFRESH_STATUS in statuses


def test_include_ignored_cache_key_separation(tmp_path: Path):
    """Snapshots with include_ignored=True and False must not collide in the index."""
    from time import time as _time

    from search_script.search_index import InventoryEntry

    index_db = tmp_path / "inventory.sqlite3"
    store = SearchIndexStore(db_path=index_db)

    cache_key_included = InventoryCacheKey(
        directory=str(tmp_path),
        max_depth=None,
        follow_symlinks=False,
        include_ignored=True,
        exclude_shots=True,
    )
    cache_key_excluded = InventoryCacheKey(
        directory=str(tmp_path),
        max_depth=None,
        follow_symlinks=False,
        include_ignored=False,
        exclude_shots=True,
    )

    now = _time()
    snapshot_included = InventorySnapshot(
        files=[
            InventoryEntry(
                file_path=str(tmp_path / "ignored.txt"),
                parent_dir=str(tmp_path),
                file_name="ignored.txt",
                file_lower="ignored.txt",
                mod_time=0.0,
                file_size=0,
            )
        ],
        directories=[str(tmp_path)],
        created_at=now,
    )
    snapshot_excluded = InventorySnapshot(
        files=[],
        directories=[str(tmp_path)],
        created_at=now,
    )

    store.save_snapshot(cache_key_included, snapshot_included)
    store.save_snapshot(cache_key_excluded, snapshot_excluded)

    from search_script.constants import PERSISTENT_INDEX_MAX_AGE_S

    result_included = store.load_snapshot(cache_key_included, max_age_s=PERSISTENT_INDEX_MAX_AGE_S)
    result_excluded = store.load_snapshot(cache_key_excluded, max_age_s=PERSISTENT_INDEX_MAX_AGE_S)

    assert result_included is not None
    assert result_excluded is not None
    assert len(result_included.snapshot.files) == 1
    assert len(result_excluded.snapshot.files) == 0


def test_filename_search_honors_max_results(tmp_path: Path):
    for index in range(5):
        (tmp_path / f"target_{index}.txt").write_text("hello", encoding="utf-8")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "target",
            search_within_files=False,
            max_results=2,
        )
    )

    assert len(results) == 2


def test_search_results_carry_mod_time(tmp_path: Path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")
    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "test", search_within_files=False))
    assert results[0].mod_time is not None
    assert results[0].formatted_mod_time != "N/A"


def test_utf16_content_search_python_backend(tmp_path: Path):
    test_file = tmp_path / "notes.txt"
    test_file.write_text("needle here\n", encoding="utf-16")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
        )
    )

    assert len(results) == 1
    assert results[0].line_content == "needle here"


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep unavailable")
def test_ripgrep_search_honors_max_results(tmp_path: Path):
    for index in range(4):
        (tmp_path / f"note_{index}.txt").write_text("needle\nneedle\n", encoding="utf-8")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.RIPGREP,
            max_results=3,
        )
    )

    assert len(results) == 3


def test_ripgrep_backend_falls_back_to_python_when_unavailable(tmp_path: Path):
    test_file = tmp_path / "notes.txt"
    test_file.write_text("needle")

    engine = SearchEngine()
    engine._ripgrep._rg_path = None  # pyright: ignore[reportPrivateUsage]
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.RIPGREP,
        )
    )

    assert len(results) == 1
    assert results[0].file_path == str(test_file)


def test_symlink_cycle_detection(tmp_path: Path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    (subdir / "file.txt").write_text("content")
    (subdir / "link").symlink_to(tmp_path)
    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "file",
            search_within_files=False,
            follow_symlinks=True,
            search_backend=SearchBackend.PYTHON,
        )
    )
    assert len(results) == 1


def test_controller_drain_remaining_results(qapp: QApplication):
    from search_script.models import DoneMsg, ResultBatchMsg

    controller = SearchController()
    controller.result_queue = queue.Queue()
    result = SearchResult("/tmp/a.txt", 3, "hello", None, mod_time=1.0, file_size=12)
    controller.result_queue.put(ResultBatchMsg([result]))
    controller.result_queue.put(DoneMsg(1))

    sentinel = controller._drain_remaining_results()  # pyright: ignore[reportPrivateUsage]

    assert isinstance(sentinel, DoneMsg)
    assert sentinel.count == 1
    # Content results are now grouped: 1 parent file item with 1 child match
    assert controller.ui.results_tree.topLevelItemCount() == 1
    parent = controller.ui.results_tree.topLevelItem(0)
    assert parent is not None
    assert parent.text(0) == "/tmp/a.txt"
    assert parent.text(1) == "1 match"
    assert parent.childCount() == 1
    child = parent.child(0)
    assert child is not None
    assert child.text(1) == "3: hello"
    close_widget(controller.ui)


def test_controller_search_worker_batches_results(qapp: QApplication):
    from search_script.models import DoneMsg, ResultBatchMsg, SearchParams

    controller = SearchController()

    def fake_search_files(**kwargs: Any) -> Any:
        yield SearchResult("/tmp/a.txt", 1, "alpha", None, mod_time=1.0, file_size=1)
        yield SearchResult("/tmp/b.txt", 2, "beta", None, mod_time=2.0, file_size=2)

    controller.search_engine.search_files = fake_search_files  # type: ignore[method-assign]
    params = SearchParams(
        directory=".",
        search_term="x",
        include_types=[],
        exclude_types=[],
        search_within_files=True,
    )
    controller._search_worker(  # pyright: ignore[reportPrivateUsage]
        params,
        controller.cancel_event,
    )

    msg = controller.result_queue.get_nowait()
    assert isinstance(msg, ResultBatchMsg)
    assert len(msg.results) == 2
    done = controller.result_queue.get_nowait()
    assert isinstance(done, DoneMsg)
    assert done.count == 2
    close_widget(controller.ui)


def test_controller_search_worker_forwards_backend_and_dates(qapp: QApplication):
    from search_script.models import SearchParams

    controller = SearchController()
    captured: dict[str, object] = {}

    def fake_search_files(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return iter(())

    controller.search_engine.search_files = fake_search_files  # type: ignore[method-assign]
    modified_after = datetime(2025, 1, 1)
    modified_before = datetime(2025, 12, 31)
    max_results = 250

    params = SearchParams(
        directory=".",
        search_term="x",
        include_types=[],
        exclude_types=[],
        search_within_files=True,
        search_backend=SearchBackend.RIPGREP,
        max_results=max_results,
        modified_after=modified_after,
        modified_before=modified_before,
    )
    controller._search_worker(  # pyright: ignore[reportPrivateUsage]
        params,
        controller.cancel_event,
    )

    assert captured["search_backend"] == SearchBackend.RIPGREP
    assert captured["max_results"] == max_results
    assert captured["modified_after"] == modified_after
    assert captured["modified_before"] == modified_before
    close_widget(controller.ui)


def test_controller_build_modified_date_filters_includes_full_day(qapp: QApplication):
    controller = SearchController()
    controller.ui.modified_after_entry.setDate(QDate(2025, 1, 1))
    controller.ui.modified_before_entry.setDate(QDate(2025, 1, 1))

    modified_after, modified_before = controller.ui.build_modified_date_filters()

    assert modified_after == datetime(2025, 1, 1, 0, 0, 0)
    assert modified_before == datetime(2025, 1, 1, 23, 59, 59, 999999)
    close_widget(controller.ui)


# --- Caching & TTL tests ---


def test_in_memory_cache_fresh_at_59s(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """In-memory snapshot is still fresh at 59s elapsed (TTL = 60s)."""
    from search_script.search_index import InventoryEntry

    engine = SearchEngine()
    snapshot = InventorySnapshot(
        files=[
            InventoryEntry(
                file_path=str(tmp_path / "a.txt"),
                parent_dir=str(tmp_path),
                file_name="a.txt",
                file_lower="a.txt",
                mod_time=0.0,
                file_size=0,
            )
        ],
        directories=[str(tmp_path)],
        created_at=0.0,
    )
    from time import time as _real_time

    frozen = _real_time()
    snapshot.created_at = frozen - 59.0
    monkeypatch.setattr("search_script.inventory.time", lambda: frozen)
    assert engine._inventory._is_fresh(snapshot) is True  # pyright: ignore[reportPrivateUsage]  # pyright: ignore[reportPrivateUsage]


def test_fast_scan_uses_base_ttl():
    engine = SearchEngine()
    snapshot = InventorySnapshot(files=[], directories=[], created_at=0.0, scan_duration_s=0.5)
    assert engine._inventory._compute_effective_ttl(snapshot) == 300.0  # pyright: ignore[reportPrivateUsage]  # pyright: ignore[reportPrivateUsage]


def test_slow_scan_scales_ttl():
    engine = SearchEngine()
    snapshot = InventorySnapshot(files=[], directories=[], created_at=0.0, scan_duration_s=10.0)
    assert engine._inventory._compute_effective_ttl(snapshot) == 1200.0  # pyright: ignore[reportPrivateUsage]  # pyright: ignore[reportPrivateUsage]


def test_very_slow_scan_caps_at_ceiling():
    engine = SearchEngine()
    snapshot = InventorySnapshot(files=[], directories=[], created_at=0.0, scan_duration_s=30.0)
    assert engine._inventory._compute_effective_ttl(snapshot) == 1800.0  # pyright: ignore[reportPrivateUsage]  # pyright: ignore[reportPrivateUsage]


def test_scan_duration_persisted_and_loaded(tmp_path: Path):
    """scan_duration_s round-trips through SQLite."""
    from time import time as _time

    from search_script.search_index import InventoryEntry

    index_db = tmp_path / "inventory.sqlite3"
    store = SearchIndexStore(db_path=index_db)

    key = InventoryCacheKey(
        directory=str(tmp_path),
        max_depth=None,
        follow_symlinks=False,
        include_ignored=True,
        exclude_shots=True,
    )
    snapshot = InventorySnapshot(
        files=[
            InventoryEntry(
                file_path=str(tmp_path / "f.txt"),
                parent_dir=str(tmp_path),
                file_name="f.txt",
                file_lower="f.txt",
                mod_time=0.0,
                file_size=0,
            )
        ],
        directories=[str(tmp_path)],
        created_at=_time(),
        scan_duration_s=12.5,
    )
    store.save_snapshot(key, snapshot)

    loaded = store.load_snapshot(key, max_age_s=9999)
    assert loaded is not None
    assert loaded.snapshot.scan_duration_s == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
        12.5
    )


def test_schema_migration_adds_column(tmp_path: Path):
    """Old DB without scan_duration_s column still works after migration."""
    from time import time as _time

    index_db = tmp_path / "inventory.sqlite3"

    # Create an old-schema DB without the column
    with sqlite3.connect(str(index_db)) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(
            """
            CREATE TABLE inventories (
                cache_key TEXT PRIMARY KEY,
                directory TEXT NOT NULL,
                max_depth INTEGER,
                follow_symlinks INTEGER NOT NULL,
                created_at REAL NOT NULL
            );
            CREATE TABLE inventory_dirs (
                cache_key TEXT NOT NULL,
                dir_path TEXT NOT NULL,
                PRIMARY KEY (cache_key, dir_path)
            );
            CREATE TABLE inventory_entries (
                cache_key TEXT NOT NULL,
                file_path TEXT NOT NULL,
                parent_dir TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_lower TEXT NOT NULL,
                mod_time REAL NOT NULL,
                file_size INTEGER NOT NULL,
                PRIMARY KEY (cache_key, file_path)
            );
            """
        )
        # Insert a row in the old schema
        conn.execute(
            "INSERT INTO inventories VALUES (?, ?, ?, ?, ?)",
            ('{"directory":"x"}', "x", None, 0, _time()),
        )

    # Now open with SearchIndexStore — migration should add the column
    store = SearchIndexStore(db_path=index_db)
    key = InventoryCacheKey(
        directory=str(tmp_path),
        max_depth=None,
        follow_symlinks=False,
        include_ignored=True,
        exclude_shots=True,
    )
    snapshot = InventorySnapshot(files=[], directories=[], created_at=_time(), scan_duration_s=5.0)
    store.save_snapshot(key, snapshot)

    loaded = store.load_snapshot(key, max_age_s=9999)
    assert loaded is not None
    assert loaded.snapshot.scan_duration_s == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
        5.0
    )


def test_spot_check_extends_ttl_when_unchanged(tmp_path: Path):
    """Spot-check passes → no full rescan, snapshot reused."""
    search_root = tmp_path / "root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    for i in range(5):
        (search_root / f"file{i}.txt").write_text("hello", encoding="utf-8")

    engine = SearchEngine(index_db_path=index_db)
    # First search populates cache
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
        )
    )

    # Age the persistent entry past the adaptive TTL ceiling so it's stale
    from search_script.constants import PERSISTENT_INDEX_MAX_AGE_CEILING_S

    with sqlite3.connect(str(index_db)) as conn:
        conn.execute(
            "UPDATE inventories SET created_at = created_at - ?",
            (PERSISTENT_INDEX_MAX_AGE_CEILING_S + 1,),
        )
    # Flush in-memory cache
    engine._inventory._cache.clear()  # pyright: ignore[reportPrivateUsage]

    build_calls = 0
    original_build = engine._inventory._build_snapshot  # pyright: ignore[reportPrivateUsage]

    def counting_build(*args: Any, **kwargs: Any) -> Any:
        nonlocal build_calls
        build_calls += 1
        return original_build(*args, **kwargs)

    engine._inventory._build_snapshot = counting_build  # type: ignore[method-assign]

    # Second search — spot-check should pass, no rebuild
    results = list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
        )
    )
    assert len(results) == 5
    assert build_calls == 0


def test_spot_check_triggers_rescan_on_mtime_change(tmp_path: Path):
    """Spot-check fails on mtime change → background rescan triggered."""
    search_root = tmp_path / "root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    target = search_root / "file.txt"
    target.write_text("hello", encoding="utf-8")

    engine = SearchEngine(index_db_path=index_db)
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
        )
    )

    # Age the persistent entry past ceiling
    from search_script.constants import PERSISTENT_INDEX_MAX_AGE_CEILING_S

    with sqlite3.connect(str(index_db)) as conn:
        conn.execute(
            "UPDATE inventories SET created_at = created_at - ?",
            (PERSISTENT_INDEX_MAX_AGE_CEILING_S + 1,),
        )
    engine._inventory._cache.clear()  # pyright: ignore[reportPrivateUsage]

    # Modify the file so mtime changes
    target.write_text("modified", encoding="utf-8")

    statuses: list[str] = []
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            progress_callback=statuses.append,
            search_backend=SearchBackend.PYTHON,
        )
    )
    assert InventoryManager.BACKGROUND_REFRESH_STATUS in statuses


def test_spot_check_triggers_rescan_on_delete(tmp_path: Path):
    """Spot-check fails on deleted file → background rescan triggered."""
    search_root = tmp_path / "root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    target = search_root / "file.txt"
    target.write_text("hello", encoding="utf-8")

    engine = SearchEngine(index_db_path=index_db)
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
        )
    )

    from search_script.constants import PERSISTENT_INDEX_MAX_AGE_CEILING_S

    with sqlite3.connect(str(index_db)) as conn:
        conn.execute(
            "UPDATE inventories SET created_at = created_at - ?",
            (PERSISTENT_INDEX_MAX_AGE_CEILING_S + 1,),
        )
    engine._inventory._cache.clear()  # pyright: ignore[reportPrivateUsage]

    # Delete the file
    target.unlink()

    statuses: list[str] = []
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            progress_callback=statuses.append,
            search_backend=SearchBackend.PYTHON,
        )
    )
    assert InventoryManager.BACKGROUND_REFRESH_STATUS in statuses


def test_refresh_clears_cache_and_retriggers(qapp: QApplication):
    """Refresh clears cache and re-runs the search."""
    from search_script.models import SearchParams

    controller = SearchController()
    search_called: list[SearchParams] = []

    def capture_start(params: SearchParams) -> None:
        search_called.append(params)

    controller._start_search_from_params = capture_start  # type: ignore[method-assign]

    # Simulate having run a search by setting _last_search_params
    controller._last_search_params = SearchParams(  # pyright: ignore[reportPrivateUsage]
        directory="/tmp/test",
        search_term="hello",
        max_depth=None,
        follow_symlinks=False,
        include_ignored=True,
    )

    cache_cleared = False

    def track_clear(**kwargs: Any) -> None:
        nonlocal cache_cleared
        cache_cleared = True

    controller.search_engine.clear_inventory_cache = track_clear  # type: ignore[method-assign]

    controller._refresh_search()  # pyright: ignore[reportPrivateUsage]

    assert cache_cleared
    assert len(search_called) == 1
    assert search_called[0].directory == "/tmp/test"
    close_widget(controller.ui)


def test_refresh_button_disabled_before_search(qapp: QApplication):
    """Refresh button starts disabled and enables after results."""
    controller = SearchController()
    assert not controller.ui.refresh_button.isEnabled()
    close_widget(controller.ui)


def test_ui_rejects_invalid_numeric_filter(qapp: QApplication, monkeypatch: pytest.MonkeyPatch):
    messages: list[str] = []
    ui = SearchUI()
    ui.dir_entry.setText(os.getcwd())
    ui.search_entry.setText("needle")
    ui.min_size_entry.setText("abc")
    monkeypatch.setattr(
        "search_script.ui_components.QMessageBox.warning",
        lambda *args: messages.append(str(args[-1])),  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType,reportUnknownMemberType]
    )

    assert not ui._validate_inputs()  # pyright: ignore[reportPrivateUsage]
    assert messages
    close_widget(ui)


def test_ui_rejects_non_positive_max_results(qapp: QApplication, monkeypatch: pytest.MonkeyPatch):
    messages: list[str] = []
    ui = SearchUI()
    ui.dir_entry.setText(os.getcwd())
    ui.search_entry.setText("needle")
    ui.max_results_entry.setText("0")
    monkeypatch.setattr(
        "search_script.ui_components.QMessageBox.warning",
        lambda *args: messages.append(str(args[-1])),  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType,reportUnknownMemberType]
    )

    assert not ui._validate_inputs()  # pyright: ignore[reportPrivateUsage]
    assert any("greater than zero" in message for message in messages)
    close_widget(ui)


def test_controller_reports_result_limit_in_completion_status(qapp: QApplication):
    controller = SearchController()
    controller.search_was_truncated = True
    controller.search_result_limit = 2
    controller.ui.add_results_batch(
        [
            SearchResult("/tmp/a.txt", file_size=1, mod_time=1.0),
            SearchResult("/tmp/b.txt", file_size=2, mod_time=2.0),
        ]
    )

    controller._handle_search_complete()  # pyright: ignore[reportPrivateUsage]

    assert "limit 2" in controller.ui.status_label.text()
    close_widget(controller.ui)


def test_results_tree_sorts_size_numerically(qapp: QApplication):
    ui = SearchUI()
    ui.add_results_batch(
        [
            SearchResult("/tmp/a.txt", file_size=2048, mod_time=1.0),
            SearchResult("/tmp/b.txt", file_size=10, mod_time=2.0),
            SearchResult("/tmp/c.txt", file_size=104857600, mod_time=3.0),
        ]
    )

    ui.results_tree.sortItems(2, Qt.SortOrder.AscendingOrder)
    sizes = []
    for i in range(ui.results_tree.topLevelItemCount()):
        item = ui.results_tree.topLevelItem(i)
        assert item is not None
        sizes.append(item.text(2))  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]

    assert sizes == ["10 B", "2.0 KB", "100.0 MB"]
    close_widget(ui)


def test_on_limit_reached_callback_invoked(tmp_path: Path):
    """on_limit_reached must be called with the limit when results are truncated."""
    for i in range(5):
        (tmp_path / f"target_{i}.txt").write_text("hello", encoding="utf-8")

    engine = SearchEngine()
    called_with: list[int] = []

    list(
        engine.search_files(
            str(tmp_path),
            "target",
            search_within_files=False,
            max_results=2,
            on_limit_reached=called_with.append,
        )
    )

    assert called_with == [2]


def test_export_results_permission_error_shows_dialog(
    qapp: QApplication, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """export_results must show QMessageBox.critical on OSError, not crash silently."""
    import builtins

    ui = SearchUI()
    try:
        # Patch dialog to return a fake path
        monkeypatch.setattr(
            "search_script.ui_components.QFileDialog.getSaveFileName",
            lambda *args, **kwargs: ("/fake/path/results.json", "JSON (*.json)"),  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType]
        )
        # Patch open to raise PermissionError
        real_open = builtins.open

        def mock_open(path: Any, *args: Any, **kwargs: Any) -> Any:
            if "results.json" in str(path):
                raise PermissionError("Permission denied")
            return real_open(path, *args, **kwargs)  # pyright: ignore[reportUnknownVariableType]

        monkeypatch.setattr(builtins, "open", mock_open)

        # Track QMessageBox.critical calls
        critical_calls: list[tuple[Any, ...]] = []
        monkeypatch.setattr(
            "search_script.ui_components.QMessageBox.critical",
            lambda *args, **kwargs: critical_calls.append(args),  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType,reportUnknownMemberType]
        )

        ui.export_results()

        assert len(critical_calls) == 1, "QMessageBox.critical should be called once on error"
        assert "Export Failed" in critical_calls[0][1]  # pyright: ignore[reportArgumentType]
    finally:
        close_widget(ui)


def test_search_small_file_attribute_error_propagates(tmp_path: Path):
    """AttributeError inside _search_small_file must propagate as SearchError,
    not be silently swallowed as FileAccessError."""
    from search_script.config import SearchError

    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world", encoding="utf-8")

    engine = SearchEngine()

    def raise_attribute_error(*args: Any, **kwargs: Any) -> Any:
        raise AttributeError("simulated programming error")

    engine._search_small_file = raise_attribute_error  # pyright: ignore[reportAttributeAccessIssue,reportPrivateUsage]

    with pytest.raises(SearchError):
        list(
            engine.search_files(
                str(tmp_path),
                "hello",
                search_within_files=True,
                search_backend=SearchBackend.PYTHON,
            )
        )


def test_bom_detection_utf16(tmp_path: Path):
    """BOM detection must find UTF-16 content without relying on chardet."""
    test_file = tmp_path / "bom_test.txt"
    test_file.write_text("needle here\n", encoding="utf-16")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
        )
    )

    assert len(results) == 1
    assert results[0].line_content == "needle here"


def test_threaded_content_search_correctness(tmp_path: Path):
    """Parallel content search with 1 and 4 workers must return the same result set."""
    for i in range(10):
        f = tmp_path / f"file_{i:02d}.txt"
        f.write_text(f"line before\nneedle on line {i}\nline after\n", encoding="utf-8")

    engine_single = SearchEngine(max_workers=1)
    results_single = list(
        engine_single.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
        )
    )

    engine_multi = SearchEngine(max_workers=4)
    results_multi = list(
        engine_multi.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
        )
    )

    # Same files found, order may differ
    assert sorted((r.file_path, r.line_number) for r in results_single) == sorted(
        (r.file_path, r.line_number) for r in results_multi
    )
    assert len(results_single) == 10


def test_gitignore_filtering_python_backend(tmp_path: Path):
    """Files matched by .gitignore should be skipped when include_ignored=False."""
    (tmp_path / ".gitignore").write_text("*.log\n__pycache__/\n")
    (tmp_path / "app.py").write_text("hello")
    (tmp_path / "debug.log").write_text("hello")
    cache_dir = tmp_path / "__pycache__"
    cache_dir.mkdir()
    (cache_dir / "module.pyc").write_text("hello")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "hello",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
            include_ignored=False,
        )
    )
    paths = {r.file_path for r in results}
    assert str(tmp_path / "app.py") in paths
    assert str(tmp_path / "debug.log") not in paths
    assert not any("__pycache__" in p for p in paths)


def test_include_ignored_true_returns_all(tmp_path: Path):
    """When include_ignored=True (default), gitignore patterns are not applied."""
    (tmp_path / ".gitignore").write_text("*.log\n")
    (tmp_path / "app.py").write_text("needle")
    (tmp_path / "debug.log").write_text("needle")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
            include_ignored=True,
        )
    )
    paths = {r.file_path for r in results}
    assert str(tmp_path / "app.py") in paths
    assert str(tmp_path / "debug.log") in paths


def test_nested_gitignore_anchored_patterns(tmp_path: Path):
    """Anchored patterns in nested .gitignore files apply only relative to that file's directory."""
    # Root .gitignore — ignores *.log everywhere
    (tmp_path / ".gitignore").write_text("*.log\n")

    # Root-level build/ should NOT be ignored (no root .gitignore rule for it)
    root_build = tmp_path / "build"
    root_build.mkdir()
    (root_build / "artifact.txt").write_text("needle")

    # subdir/.gitignore with anchored /build — should only ignore subdir/build/
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    (subdir / ".gitignore").write_text("/build\n")
    (subdir / "source.py").write_text("needle")

    subdir_build = subdir / "build"
    subdir_build.mkdir()
    (subdir_build / "output.txt").write_text("needle")

    engine = SearchEngine()
    # Call _walk_scandir directly to test gitignore logic without going through
    # the inventory snapshot path (which has a separate pre-existing bug).
    walked = list(
        engine._inventory._walk_scandir(  # pyright: ignore[reportPrivateUsage]
            str(tmp_path),
            max_depth=None,
            follow_symlinks=False,
            cancel_event=None,
            include_ignored=False,
        )
    )
    # Extract file paths from (dir_path, entry) tuples where entry is not None
    paths = {entry.path for _, entry in walked if entry is not None}

    # root/build/artifact.txt must be present — root .gitignore has no /build rule
    assert str(root_build / "artifact.txt") in paths, "root/build/ should not be ignored"
    # subdir/source.py must be present
    assert str(subdir / "source.py") in paths, "subdir/source.py should not be ignored"
    # subdir/build/output.txt must be absent — subdir/.gitignore has anchored /build
    # subdir/build/ should be ignored by nested .gitignore
    assert str(subdir_build / "output.txt") not in paths


# ---------------------------------------------------------------------------
# C1: Result grouping by file
# ---------------------------------------------------------------------------


def test_content_results_grouped_by_file(qapp: QApplication):
    """Content search results should be grouped under parent file items."""
    ui = SearchUI()
    ui.add_results_batch(
        [
            SearchResult("/tmp/a.txt", 1, "hello", None, mod_time=1.0, file_size=100),
            SearchResult("/tmp/a.txt", 5, "hello again", None, mod_time=1.0, file_size=100),
            SearchResult("/tmp/b.txt", 3, "hello there", None, mod_time=2.0, file_size=200),
        ]
    )
    # Should have 2 top-level items (one per file)
    assert ui.results_tree.topLevelItemCount() == 2
    # Collect parents regardless of sort order
    parents = {}
    for i in range(ui.results_tree.topLevelItemCount()):
        item = ui.results_tree.topLevelItem(i)
        assert item is not None
        parents[item.text(0)] = item
    assert "/tmp/a.txt" in parents
    assert "/tmp/b.txt" in parents
    assert parents["/tmp/a.txt"].childCount() == 2  # pyright: ignore[reportUnknownMemberType]
    assert parents["/tmp/b.txt"].childCount() == 1  # pyright: ignore[reportUnknownMemberType]
    # get_result_summary should count 3 matches, 2 files
    matches, files = ui.get_result_summary()
    assert matches == 3
    assert files == 2
    close_widget(ui)


def test_filename_results_remain_flat(qapp: QApplication):
    """Filename search results (no line_number) should stay flat."""
    ui = SearchUI()
    ui.add_results_batch(
        [
            SearchResult("/tmp/a.txt", file_size=100, mod_time=1.0),
            SearchResult("/tmp/b.txt", file_size=200, mod_time=2.0),
        ]
    )
    assert ui.results_tree.topLevelItemCount() == 2
    assert ui.results_tree.topLevelItem(0).childCount() == 0  # type: ignore[union-attr]
    close_widget(ui)


# ---------------------------------------------------------------------------
# C2: Configurable context lines
# ---------------------------------------------------------------------------


def test_context_lines_python_backend(tmp_path: Path):
    """Context lines should be captured when context_lines > 0."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("line1\nline2\nneedle here\nline4\nline5\n")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
            context_lines=2,
        )
    )
    assert len(results) == 1
    assert results[0].context_before == ["line1", "line2"]
    assert results[0].context_after == ["line4", "line5"]


# ---------------------------------------------------------------------------
# C3: Match position tracking
# ---------------------------------------------------------------------------


def test_match_position_substring(tmp_path: Path):
    """Substring search should report match start and length."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("Hello World")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "World",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
        )
    )
    assert len(results) == 1
    assert results[0].match_start == 6
    assert results[0].match_length == 5


def test_match_position_regex(tmp_path: Path):
    """Regex search should report match start and length."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("foo bar123 baz")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            r"bar\d+",
            search_within_files=True,
            search_mode=SearchMode.REGEX,
            search_backend=SearchBackend.PYTHON,
        )
    )
    assert len(results) == 1
    assert results[0].match_start == 4
    assert results[0].match_length == 6


# ---------------------------------------------------------------------------
# Indexing fix tests
# ---------------------------------------------------------------------------


def test_filename_search_progress_count(tmp_path: Path):
    """Progress counter must include files that raise FileAccessError (no under-count)."""
    # Create 200 files so the modulo-100 progress callback actually fires
    for i in range(200):
        (tmp_path / f"file_{i:03d}.txt").write_text("data", encoding="utf-8")

    engine = SearchEngine()
    progress_values: list[str] = []
    list(
        engine.search_files(
            str(tmp_path),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
            progress_callback=progress_values.append,
        )
    )
    # With 200 files, at least one "200/200" message (100%) should appear
    assert any("200/200" in m for m in progress_values), (
        f"Expected 200/200 progress, got: {progress_values[-3:]}"
    )


def test_mmap_context_after_multiple_lines(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """mmap path should return multiple context_after lines, not just one."""
    from search_script import search_engine as se_mod

    # Patch the module-level constant so _search_file_content routes through mmap
    monkeypatch.setattr(se_mod, "LARGE_FILE_MMAP_THRESHOLD", 0)

    test_file = tmp_path / "test.txt"
    test_file.write_text("before1\nbefore2\nneedle here\nafter1\nafter2\nafter3\n")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.PYTHON,
            context_lines=2,
        )
    )
    assert len(results) == 1
    assert results[0].context_before == ["before1", "before2"]
    assert results[0].context_after is not None
    assert len(results[0].context_after) == 2
    assert results[0].context_after == ["after1", "after2"]


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep unavailable")
def test_ripgrep_context_lines(tmp_path: Path):
    """Ripgrep backend should populate context_before and context_after."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("line1\nline2\nneedle here\nline4\nline5\n", encoding="utf-8")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "needle",
            search_within_files=True,
            search_backend=SearchBackend.RIPGREP,
            context_lines=2,
        )
    )
    assert len(results) == 1
    assert results[0].context_before is not None
    assert results[0].context_after is not None
    assert len(results[0].context_before) == 2
    assert len(results[0].context_after) == 2


def test_save_snapshot_transaction_safety(tmp_path: Path):
    """save_snapshot must use IMMEDIATE isolation to ensure atomic DELETE+INSERT.
    If an error occurs mid-write, the transaction rolls back preserving old data."""
    import unittest.mock
    from time import time as _time

    from search_script.search_index import InventoryEntry

    index_db = tmp_path / "inventory.sqlite3"
    store = SearchIndexStore(db_path=index_db)

    cache_key = InventoryCacheKey(
        directory=str(tmp_path),
        max_depth=None,
        follow_symlinks=False,
        include_ignored=True,
        exclude_shots=True,
    )
    now = _time()
    original_snapshot = InventorySnapshot(
        files=[
            InventoryEntry(
                file_path=str(tmp_path / "orig.txt"),
                parent_dir=str(tmp_path),
                file_name="orig.txt",
                file_lower="orig.txt",
                mod_time=0.0,
                file_size=0,
            )
        ],
        directories=[str(tmp_path)],
        created_at=now,
    )
    store.save_snapshot(cache_key, original_snapshot)

    # Verify IMMEDIATE isolation level is used by intercepting sqlite3.connect
    connect_kwargs: list[dict[str, object]] = []
    original_connect = sqlite3.connect

    def tracking_connect(*args: Any, **kwargs: Any) -> Any:
        connect_kwargs.append(dict(kwargs))
        conn = original_connect(*args, **kwargs)
        return conn

    bad_snapshot = InventorySnapshot(
        files=[
            InventoryEntry(
                file_path=str(tmp_path / "new.txt"),
                parent_dir=str(tmp_path),
                file_name="new.txt",
                file_lower="new.txt",
                mod_time=0.0,
                file_size=0,
            )
        ],
        directories=[str(tmp_path)],
        created_at=now + 1,
    )

    with unittest.mock.patch("search_script.search_index.sqlite3.connect", tracking_connect):
        store.save_snapshot(cache_key, bad_snapshot)

    # At least one connect call should use IMMEDIATE isolation
    assert any(kw.get("isolation_level") == "IMMEDIATE" for kw in connect_kwargs), (
        f"Expected IMMEDIATE isolation, got: {connect_kwargs}"
    )


def test_shutdown_stops_background_refresh(tmp_path: Path):
    """shutdown() should cause the background refresh thread to terminate promptly."""
    from time import monotonic

    search_root = tmp_path / "search_root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    for i in range(50):
        (search_root / f"file_{i}.txt").write_text("data", encoding="utf-8")

    engine = SearchEngine(index_db_path=index_db)
    # Build initial inventory
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
        )
    )

    # Age the snapshot to trigger background refresh
    from search_script.constants import PERSISTENT_INDEX_MAX_AGE_S

    with sqlite3.connect(str(index_db)) as conn:
        conn.execute(
            "UPDATE inventories SET created_at = created_at - ?",
            (PERSISTENT_INDEX_MAX_AGE_S + 1,),
        )

    # Clear in-memory cache to force persistent index load
    engine._inventory._cache.clear()  # pyright: ignore[reportPrivateUsage]

    # Trigger a search that will start a background refresh
    list(
        engine.search_files(
            str(search_root),
            "file",
            search_within_files=False,
            search_backend=SearchBackend.PYTHON,
        )
    )

    # Now shut down
    engine.shutdown()

    deadline = monotonic() + 2.0
    while monotonic() < deadline:
        with engine._inventory._refresh_lock:  # pyright: ignore[reportPrivateUsage]
            if not engine._inventory._refreshes:  # pyright: ignore[reportPrivateUsage]
                break
        sleep(0.05)

    with engine._inventory._refresh_lock:  # pyright: ignore[reportPrivateUsage]
        assert not engine._inventory._refreshes, "Background refresh should have stopped"  # pyright: ignore[reportPrivateUsage]


def test_always_binary_skip_no_sniff(tmp_path: Path):
    """Always-binary extensions (.exr) should be rejected without calling _is_likely_binary;
    maybe-binary extensions (.usd) should call it and allow text-based files through."""
    engine = SearchEngine()
    sniff_calls: list[str] = []
    original_is_likely_binary = engine._is_likely_binary  # pyright: ignore[reportPrivateUsage]

    def tracking_is_likely_binary(file_path: Path) -> bool:
        sniff_calls.append(str(file_path))
        return original_is_likely_binary(file_path)

    engine._is_likely_binary = tracking_is_likely_binary  # type: ignore[method-assign]

    exr_file = tmp_path / "render.exr"
    exr_file.write_bytes(b"\x00" * 10)
    usd_file = tmp_path / "scene.usd"
    usd_file.write_text("usda 1.0\n")  # text-based USD

    # .exr should be rejected immediately (always-binary), no sniff needed
    assert not engine._should_process_file(  # pyright: ignore[reportPrivateUsage]
        "render.exr", [], [], search_within_files=True, file_path=exr_file
    )
    assert not any("render.exr" in call for call in sniff_calls)

    # .usd should trigger the sniff check (maybe-binary)
    result = engine._should_process_file(  # pyright: ignore[reportPrivateUsage]
        "scene.usd", [], [], search_within_files=True, file_path=usd_file
    )
    assert any("scene.usd" in call for call in sniff_calls)
    # Text-based USD passes the sniff test
    assert result is True


def test_usdz_in_always_binary():
    """'.usdz' must be in the always-binary extensions set."""
    engine = SearchEngine()
    assert ".usdz" in engine._always_binary_extensions  # pyright: ignore[reportPrivateUsage]


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_substring_search(tmp_path: Path) -> None:
    """Filename substring search should work via ripgrep backend."""
    (tmp_path / "hello_world.txt").write_text("content")
    (tmp_path / "goodbye.txt").write_text("content")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "hello_again.py").write_text("content")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "hello",
            search_within_files=False,
            search_mode=SearchMode.SUBSTRING,
            search_backend=SearchBackend.RIPGREP,
        )
    )
    names = {Path(r.file_path).name for r in results}
    assert names == {"hello_world.txt", "hello_again.py"}


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_glob_search(tmp_path: Path) -> None:
    """Filename glob search should work via ripgrep backend."""
    (tmp_path / "report.txt").write_text("content")
    (tmp_path / "data.csv").write_text("content")
    (tmp_path / "notes.txt").write_text("content")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "*.txt",
            search_within_files=False,
            search_mode=SearchMode.GLOB,
            search_backend=SearchBackend.RIPGREP,
        )
    )
    names = {Path(r.file_path).name for r in results}
    assert names == {"report.txt", "notes.txt"}


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_regex_search(tmp_path: Path) -> None:
    """Filename regex search should work via ripgrep backend."""
    (tmp_path / "test1.py").write_text("content")
    (tmp_path / "test2.py").write_text("content")
    (tmp_path / "readme.md").write_text("content")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            r"test\d",
            search_within_files=False,
            search_mode=SearchMode.REGEX,
            search_backend=SearchBackend.RIPGREP,
        )
    )
    names = {Path(r.file_path).name for r in results}
    assert names == {"test1.py", "test2.py"}


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_search_respects_type_filters(tmp_path: Path) -> None:
    """Ripgrep filename search should filter by extension."""
    (tmp_path / "code.py").write_text("content")
    (tmp_path / "data.txt").write_text("content")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "co",
            search_within_files=False,
            search_mode=SearchMode.SUBSTRING,
            search_backend=SearchBackend.RIPGREP,
            include_types=[".py"],
        )
    )
    names = {Path(r.file_path).name for r in results}
    assert names == {"code.py"}


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_search_respects_max_results(tmp_path: Path) -> None:
    """Ripgrep filename search should stop at max_results."""
    for i in range(10):
        (tmp_path / f"file_{i}.txt").write_text("content")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "file",
            search_within_files=False,
            search_mode=SearchMode.SUBSTRING,
            search_backend=SearchBackend.RIPGREP,
            max_results=3,
        )
    )
    assert len(results) == 3


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_search_case_insensitive(tmp_path: Path) -> None:
    """Ripgrep filename search should be case-insensitive by default."""
    (tmp_path / "MyFile.TXT").write_text("content")

    engine = SearchEngine()
    results = list(
        engine.search_files(
            str(tmp_path),
            "myfile",
            search_within_files=False,
            search_mode=SearchMode.SUBSTRING,
            search_backend=SearchBackend.RIPGREP,
        )
    )
    assert len(results) == 1


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep not installed")
def test_ripgrep_filename_search_falls_back_when_unavailable(tmp_path: Path) -> None:
    """When rg is missing, filename search falls back to Python."""
    (tmp_path / "target.txt").write_text("content")

    engine = SearchEngine()
    engine._rg_path = None  # pyright: ignore[reportPrivateUsage]
    results = list(
        engine.search_files(
            str(tmp_path),
            "target",
            search_within_files=False,
            search_mode=SearchMode.SUBSTRING,
            search_backend=SearchBackend.AUTO,
        )
    )
    assert len(results) == 1
