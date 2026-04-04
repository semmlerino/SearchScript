"""Tests for the search application components."""

import gc
import os
import queue
import shutil
from datetime import datetime, timedelta
from time import monotonic, sleep
from typing import cast

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtCore import QDate, Qt
from PySide6.QtWidgets import QApplication

from search_script.config import ValidationError
from search_script.file_utils import FileOperations
from search_script.search_controller import SearchController
from search_script.search_engine import SearchBackend, SearchEngine, SearchMode, SearchResult
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


def close_widget(widget) -> None:
    widget.close()
    widget.deleteLater()
    app = QApplication.instance()
    if app is not None:
        app.processEvents()


def test_filename_search(tmp_path):
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


def test_content_search(tmp_path):
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
def test_large_file_content_search_python_backend(tmp_path, mode, term):
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


def test_content_search_with_type_filter(tmp_path):
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
def test_ripgrep_glob_backend_matches_content(tmp_path):
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
def test_ripgrep_backend_respects_modified_date_filter(tmp_path):
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


def test_invalid_regex_raises_validation_error(tmp_path):
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


def test_fuzzy_filename_search(tmp_path):
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


def test_fuzzy_filename_search_ranks_relevant_matches(tmp_path):
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


def test_fuzzy_content_search(tmp_path):
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


def test_filename_search_binary_extension(tmp_path):
    exr_file = tmp_path / "render.exr"
    exr_file.write_bytes(b"\x00" * 10)
    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "render", search_within_files=False))
    assert len(results) == 1
    assert results[0].file_path.endswith(".exr")


def test_search_engine_reuses_inventory_cache(tmp_path, monkeypatch):
    for index in range(3):
        (tmp_path / f"file{index}.txt").write_text("hello", encoding="utf-8")

    engine = SearchEngine()
    build_calls = 0
    original = engine._build_inventory_snapshot

    def wrapped(*args, **kwargs):
        nonlocal build_calls
        build_calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(engine, "_build_inventory_snapshot", wrapped)

    list(engine.search_files(str(tmp_path), "file", search_within_files=False))
    list(engine.search_files(str(tmp_path), "file", search_within_files=False))

    assert build_calls == 1


def test_search_engine_uses_persistent_index_between_instances(tmp_path, monkeypatch):
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

    def fail_build(*args, **kwargs):
        raise AssertionError("persistent index should avoid rebuilding inventory")

    monkeypatch.setattr(second_engine, "_build_inventory_snapshot", fail_build)
    second_results = list(
        second_engine.search_files(str(search_root), "file", search_within_files=False)
    )

    assert len(second_results) == 3


def test_search_engine_refreshes_stale_persistent_index_in_background(tmp_path):
    search_root = tmp_path / "search_root"
    search_root.mkdir()
    index_db = tmp_path / "inventory.sqlite3"
    (search_root / "one.txt").write_text("hello", encoding="utf-8")

    first_engine = SearchEngine(index_db_path=index_db)
    list(first_engine.search_files(str(search_root), "txt", search_within_files=False))

    (search_root / "two.txt").write_text("hello", encoding="utf-8")

    statuses: list[str] = []
    second_engine = SearchEngine(index_db_path=index_db)
    stale_results = list(
        second_engine.search_files(
            str(search_root),
            "txt",
            search_within_files=False,
            progress_callback=statuses.append,
        )
    )

    deadline = monotonic() + 2.0
    while monotonic() < deadline:
        with second_engine._inventory_refresh_lock:
            if not second_engine._inventory_refreshes:
                break
        sleep(0.05)

    with second_engine._inventory_refresh_lock:
        assert not second_engine._inventory_refreshes

    third_engine = SearchEngine(index_db_path=index_db)
    fresh_results = list(
        third_engine.search_files(str(search_root), "txt", search_within_files=False)
    )

    assert len(stale_results) == 1
    assert len(fresh_results) == 2
    assert second_engine.BACKGROUND_REFRESH_STATUS in statuses


def test_filename_search_honors_max_results(tmp_path):
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


def test_search_results_carry_mod_time(tmp_path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")
    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "test", search_within_files=False))
    assert results[0].mod_time is not None
    assert results[0].formatted_mod_time != "N/A"


def test_utf16_content_search_python_backend(tmp_path):
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
def test_ripgrep_search_honors_max_results(tmp_path):
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


def test_ripgrep_backend_falls_back_to_python_when_unavailable(tmp_path):
    test_file = tmp_path / "notes.txt"
    test_file.write_text("needle")

    engine = SearchEngine()
    engine._rg_path = None
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


def test_symlink_cycle_detection(tmp_path):
    subdir = tmp_path / "sub"
    subdir.mkdir()
    (subdir / "file.txt").write_text("content")
    (subdir / "link").symlink_to(tmp_path)
    engine = SearchEngine()
    results = list(
        engine.search_files(str(tmp_path), "file", search_within_files=False, follow_symlinks=True)
    )
    assert len(results) == 1


def test_controller_drain_remaining_results(qapp):
    controller = SearchController()
    controller.result_queue = queue.Queue()
    result = SearchResult("/tmp/a.txt", 3, "hello", None, mod_time=1.0, file_size=12)
    controller.result_queue.put(("result", result))
    controller.result_queue.put(("done", 1))

    sentinel = controller._drain_remaining_results()

    assert sentinel == ("done", 1)
    assert controller.ui.results_tree.topLevelItemCount() == 1
    row = controller.ui.results_tree.topLevelItem(0)
    assert row is not None
    assert [row.text(i) for i in range(4)] == [
        "/tmp/a.txt",
        "3: hello",
        "12 B",
        result.formatted_mod_time,
    ]
    close_widget(controller.ui)


def test_controller_search_worker_batches_results(qapp):
    controller = SearchController()

    def fake_search_files(**kwargs):
        yield SearchResult("/tmp/a.txt", 1, "alpha", None, mod_time=1.0, file_size=1)
        yield SearchResult("/tmp/b.txt", 2, "beta", None, mod_time=2.0, file_size=2)

    controller.search_engine.search_files = fake_search_files  # type: ignore[method-assign]
    controller._search_worker(
        {
            "directory": ".",
            "search_term": "x",
            "include_types": [],
            "exclude_types": [],
            "search_within_files": True,
            "search_mode": "substring",
            "search_backend": "python",
            "max_depth": None,
            "min_size": None,
            "max_size": None,
            "max_results": None,
            "modified_after": None,
            "modified_before": None,
            "match_folders": False,
            "follow_symlinks": False,
        }
    )

    msg_type, batch = controller.result_queue.get_nowait()
    assert msg_type == "results_batch"
    assert isinstance(batch, list)
    assert len(batch) == 2
    assert controller.result_queue.get_nowait() == ("done", 2)
    close_widget(controller.ui)


def test_controller_search_worker_forwards_backend_and_dates(qapp):
    controller = SearchController()
    captured: dict[str, object] = {}

    def fake_search_files(**kwargs):
        captured.update(kwargs)
        return iter(())

    controller.search_engine.search_files = fake_search_files  # type: ignore[method-assign]
    modified_after = datetime(2025, 1, 1)
    modified_before = datetime(2025, 12, 31)
    max_results = 250

    controller._search_worker(
        {
            "directory": ".",
            "search_term": "x",
            "include_types": [],
            "exclude_types": [],
            "search_within_files": True,
            "search_mode": "substring",
            "search_backend": "ripgrep",
            "max_depth": None,
            "min_size": None,
            "max_size": None,
            "max_results": max_results,
            "modified_after": modified_after,
            "modified_before": modified_before,
            "match_folders": False,
            "follow_symlinks": False,
        }
    )

    assert captured["search_backend"] == SearchBackend.RIPGREP
    assert captured["max_results"] == max_results
    assert captured["modified_after"] == modified_after
    assert captured["modified_before"] == modified_before
    close_widget(controller.ui)


def test_controller_build_modified_date_filters_includes_full_day(qapp):
    controller = SearchController()
    controller.ui.modified_after_entry.setDate(QDate(2025, 1, 1))
    controller.ui.modified_before_entry.setDate(QDate(2025, 1, 1))

    modified_after, modified_before = controller._build_modified_date_filters()

    assert modified_after == datetime(2025, 1, 1, 0, 0, 0)
    assert modified_before == datetime(2025, 1, 1, 23, 59, 59, 999999)
    close_widget(controller.ui)


def test_ui_rejects_invalid_numeric_filter(qapp, monkeypatch):
    messages: list[str] = []
    ui = SearchUI()
    ui.dir_entry.setText(os.getcwd())
    ui.search_entry.setText("needle")
    ui.min_size_entry.setText("abc")
    monkeypatch.setattr(
        "search_script.ui_components.QMessageBox.warning",
        lambda *args: messages.append(str(args[-1])),
    )

    assert not ui._validate_inputs()
    assert messages
    close_widget(ui)


def test_ui_rejects_non_positive_max_results(qapp, monkeypatch):
    messages: list[str] = []
    ui = SearchUI()
    ui.dir_entry.setText(os.getcwd())
    ui.search_entry.setText("needle")
    ui.max_results_entry.setText("0")
    monkeypatch.setattr(
        "search_script.ui_components.QMessageBox.warning",
        lambda *args: messages.append(str(args[-1])),
    )

    assert not ui._validate_inputs()
    assert any("greater than zero" in message for message in messages)
    close_widget(ui)


def test_controller_reports_result_limit_in_completion_status(qapp):
    controller = SearchController()
    controller.search_was_truncated = True
    controller.search_result_limit = 2
    controller.ui.add_results_batch(
        [
            SearchResult("/tmp/a.txt", file_size=1, mod_time=1.0),
            SearchResult("/tmp/b.txt", file_size=2, mod_time=2.0),
        ]
    )

    controller._handle_search_complete()

    assert "limit 2" in controller.ui.status_label.text()
    close_widget(controller.ui)


def test_results_tree_sorts_size_numerically(qapp):
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
        sizes.append(item.text(2))

    assert sizes == ["10 B", "2.0 KB", "100.0 MB"]
    close_widget(ui)


def test_on_limit_reached_callback_invoked(tmp_path):
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


def test_export_results_permission_error_shows_dialog(qapp, tmp_path, monkeypatch):
    """export_results must show QMessageBox.critical on OSError, not crash silently."""
    import builtins

    ui = SearchUI()
    try:
        # Patch dialog to return a fake path
        monkeypatch.setattr(
            "search_script.ui_components.QFileDialog.getSaveFileName",
            lambda *args, **kwargs: ("/fake/path/results.json", "JSON (*.json)"),
        )
        # Patch open to raise PermissionError
        real_open = builtins.open

        def mock_open(path, *args, **kwargs):
            if "results.json" in str(path):
                raise PermissionError("Permission denied")
            return real_open(path, *args, **kwargs)

        monkeypatch.setattr(builtins, "open", mock_open)

        # Track QMessageBox.critical calls
        critical_calls: list[tuple] = []
        monkeypatch.setattr(
            "search_script.ui_components.QMessageBox.critical",
            lambda *args, **kwargs: critical_calls.append(args),
        )

        ui.export_results()

        assert len(critical_calls) == 1, "QMessageBox.critical should be called once on error"
        assert "Export Failed" in critical_calls[0][1]
    finally:
        close_widget(ui)


def test_search_small_file_attribute_error_propagates(tmp_path):
    """AttributeError inside _search_small_file must propagate as SearchError,
    not be silently swallowed as FileAccessError."""
    from search_script.config import SearchError

    test_file = tmp_path / "test.txt"
    test_file.write_text("hello world", encoding="utf-8")

    engine = SearchEngine()

    def raise_attribute_error(*args, **kwargs):
        raise AttributeError("simulated programming error")

    engine._search_small_file = raise_attribute_error  # pyright: ignore[reportAttributeAccessIssue]

    with pytest.raises(SearchError):
        list(
            engine.search_files(
                str(tmp_path),
                "hello",
                search_within_files=True,
                search_backend=SearchBackend.PYTHON,
            )
        )


def test_bom_detection_utf16(tmp_path):
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


def test_threaded_content_search_correctness(tmp_path):
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


def test_gitignore_filtering_python_backend(tmp_path):
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


def test_include_ignored_true_returns_all(tmp_path):
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
