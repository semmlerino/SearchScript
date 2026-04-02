"""Tests for the search application components."""

import os
import queue
import shutil
from datetime import datetime, timedelta
from typing import cast

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PySide6.QtWidgets import QApplication

from search_script.file_utils import FileOperations
from search_script.search_controller import SearchController
from search_script.search_engine import SearchBackend, SearchEngine, SearchMode, SearchResult


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    qt_app = cast(QApplication, app)
    yield qt_app
    qt_app.closeAllWindows()
    qt_app.processEvents()


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


def test_search_results_carry_mod_time(tmp_path):
    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")
    engine = SearchEngine()
    results = list(engine.search_files(str(tmp_path), "test", search_within_files=False))
    assert results[0].mod_time is not None
    assert results[0].formatted_mod_time != "N/A"


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
    controller.ui.close()


def test_controller_search_worker_forwards_backend_and_dates(qapp):
    controller = SearchController()
    captured: dict[str, object] = {}

    def fake_search_files(**kwargs):
        captured.update(kwargs)
        return iter(())

    controller.search_engine.search_files = fake_search_files  # type: ignore[method-assign]
    modified_after = datetime(2025, 1, 1)
    modified_before = datetime(2025, 12, 31)

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
            "modified_after": modified_after,
            "modified_before": modified_before,
            "match_folders": False,
            "follow_symlinks": False,
        }
    )

    assert captured["search_backend"] == SearchBackend.RIPGREP
    assert captured["modified_after"] == modified_after
    assert captured["modified_before"] == modified_before
    controller.ui.close()


