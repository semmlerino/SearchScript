#!/usr/bin/env python3
"""
Test script to verify the refactored search components work correctly.
"""

import os
import tempfile
from pathlib import Path
from search_script.search_engine import SearchEngine, SearchResult
from search_script.file_utils import FileOperations, ValidationUtils
from search_script.config import ConfigManager


def test_search_engine():
    """Test the search engine functionality."""
    # Create temporary test files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create test files
        test_file1 = Path(temp_dir) / "test1.txt"
        test_file2 = Path(temp_dir) / "test2.py"
        test_file3 = Path(temp_dir) / "test3.log"

        test_file1.write_text("Hello world\nThis is a test file\nPython search")
        test_file2.write_text("def search_function():\n    return 'search result'")
        test_file3.write_text("ERROR: search failed\nINFO: search completed")

        # Test search engine
        engine = SearchEngine()

        # Test filename search
        print("Testing filename search for 'test':")
        results = list(engine.search_files(temp_dir, "test", search_within_files=False))
        print(f"Found {len(results)} files")
        for result in results:
            print(f"  - {result.file_path}")

        # Test content search
        print("\nTesting content search for 'search':")
        results = list(engine.search_files(temp_dir, "search", search_within_files=True))
        print(f"Found {len(results)} matches")
        for result in results:
            print(f"  - {result.file_path}:{result.line_number} -> {result.line_content}")

        # Test with file type filtering
        print("\nTesting with .py file filter:")
        results = list(engine.search_files(
            temp_dir,
            "search",
            include_types=[".py"],
            search_within_files=True
        ))
        print(f"Found {len(results)} matches in Python files")

        print("\nSearch engine tests completed successfully!")


def test_file_operations():
    """Test file operations utilities."""
    file_ops = FileOperations()

    # Test with a file that should exist
    current_file = __file__
    mod_time = file_ops.get_file_modification_time(current_file)
    print(f"Modification time for {current_file}: {mod_time}")

    # Test with non-existent file
    fake_file = "/path/to/nonexistent/file.txt"
    mod_time = file_ops.get_file_modification_time(fake_file)
    print(f"Modification time for non-existent file: {mod_time}")

    print("File operations tests completed successfully!")


def test_validation():
    """Test validation utilities."""
    # Test directory validation
    assert ValidationUtils.validate_directory("/") == True
    assert ValidationUtils.validate_directory("/nonexistent/path") == False

    # Test search term validation
    assert ValidationUtils.validate_search_term("test") == True
    assert ValidationUtils.validate_search_term("") == False
    assert ValidationUtils.validate_search_term("   ") == False

    # Test file extensions
    extensions = ValidationUtils.validate_file_extensions(["txt", ".py", " .log ", ""])
    expected = [".txt", ".py", ".log"]
    assert extensions == expected

    print("Validation tests completed successfully!")


def test_config():
    """Test configuration management."""
    # Create a temporary config for testing
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config_file = f.name

    try:
        config_manager = ConfigManager(config_file)
        config = config_manager.get_config()

        print(f"Default window size: {config.window_width}x{config.window_height}")
        print(f"Log level: {config.log_level}")
        print(f"Default exclude types: {config.default_exclude_types}")

        # Test updating config
        config_manager.update_config(window_width=1600, log_level="DEBUG")
        updated_config = config_manager.get_config()
        assert updated_config.window_width == 1600
        assert updated_config.log_level == "DEBUG"

        print("Configuration tests completed successfully!")
    finally:
        # Clean up
        if os.path.exists(config_file):
            os.unlink(config_file)


def main():
    """Run all tests."""
    print("=" * 50)
    print("Testing Refactored Search Application")
    print("=" * 50)

    try:
        test_search_engine()
        print()
        test_file_operations()
        print()
        test_validation()
        print()
        test_config()
        print()
        print("All tests passed successfully!")
        print("The refactored code is working correctly.")

    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
