"""Tests for the search application components."""

import os
from pathlib import Path

from search_script.search_engine import SearchEngine, SearchResult
from search_script.file_utils import FileOperations, ValidationUtils
from search_script.config import ConfigManager


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
    assert "search" in results[0].line_content.lower()


def test_content_search_with_type_filter(tmp_path):
    (tmp_path / "code.py").write_text("def search_function(): pass")
    (tmp_path / "notes.txt").write_text("search notes here")

    engine = SearchEngine()
    results = list(engine.search_files(
        str(tmp_path), "search", include_types=[".py"], search_within_files=True
    ))
    assert len(results) == 1
    assert results[0].file_path.endswith(".py")


def test_file_modification_time():
    file_ops = FileOperations()
    mod_time = file_ops.get_file_modification_time(__file__)
    assert mod_time != "N/A"

    mod_time = file_ops.get_file_modification_time("/path/to/nonexistent/file.txt")
    assert mod_time == "N/A"


def test_validate_directory():
    assert ValidationUtils.validate_directory("/") is True
    assert ValidationUtils.validate_directory("/nonexistent/path") is False


def test_validate_search_term():
    assert ValidationUtils.validate_search_term("test") is True
    assert ValidationUtils.validate_search_term("") is False
    assert ValidationUtils.validate_search_term("   ") is False


def test_validate_file_extensions():
    extensions = ValidationUtils.validate_file_extensions(["txt", ".py", " .log ", ""])
    assert extensions == [".txt", ".py", ".log"]


def test_config_manager(tmp_path):
    config_file = str(tmp_path / "test_config.json")
    manager = ConfigManager(config_file)
    config = manager.get_config()

    assert config.window_width == 1200
    assert config.window_height == 700

    manager.update_config(window_width=1600, log_level="DEBUG")
    updated = manager.get_config()
    assert updated.window_width == 1600
    assert updated.log_level == "DEBUG"
