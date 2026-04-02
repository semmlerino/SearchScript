#!/usr/bin/env python3
"""Bundle application files for base64 encoding.

This script collects all relevant application files (respecting .gitignore),
copies them to a temporary directory, and optionally encodes them using transfer_cli.py.
"""

from __future__ import annotations

# Standard library imports
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict, cast


class BundleConfig(TypedDict, total=False):
    """Type definition for bundle configuration."""

    include_patterns: list[str]
    exclude_patterns: list[str]
    exclude_dirs: list[str]
    max_file_size_mb: int
    chunk_size_kb: int
    output_dir: str


class GitIgnoreParser:
    """Parse and apply .gitignore patterns."""

    def __init__(self, gitignore_path: str | None = None) -> None:
        """Initialize with optional .gitignore file path."""
        super().__init__()
        self.patterns: list[str] = []
        self.always_exclude: set[str] = {
            "__pycache__",
            ".git",
            ".pytest_cache",
            "venv",
            "env",
            ".venv",
            ".env",
            "*.pyc",
            "*.pyo",
            "*.pyd",
            ".DS_Store",
            "Thumbs.db",
            ".coverage",
            "htmlcov",
            ".hypothesis",
        }

        if gitignore_path and Path(gitignore_path).exists():
            self._parse_gitignore(gitignore_path)

    def _parse_gitignore(self, gitignore_path: str) -> None:
        """Parse .gitignore file and extract patterns."""
        with Path(gitignore_path).open() as f:
            for line in f:
                stripped_line = line.strip()
                # Skip comments and empty lines
                if stripped_line and not stripped_line.startswith("#"):
                    self.patterns.append(stripped_line)

    def should_exclude(self, path: str, is_dir: bool = False) -> bool:  # skylos: ignore
        """Check if a path should be excluded based on patterns.

        Args:
            path: Relative path to check
            is_dir: Whether the path is a directory

        Returns:
            True if the path should be excluded

        """
        path_parts = Path(path).parts
        path_name = Path(path).name

        # Check always exclude patterns
        for pattern in self.always_exclude:
            if pattern.startswith("*."):
                # File extension pattern
                extension = pattern[1:]
                if path.endswith(extension) or path_name.endswith(extension):
                    return True
            elif pattern in path_parts or path_name == pattern:
                return True

        # Check gitignore patterns
        for pattern in self.patterns:
            # Simple pattern matching (not full gitignore spec)
            if pattern.endswith("/"):
                # Directory pattern
                if is_dir and (pattern[:-1] in path_parts or path_name == pattern[:-1]):
                    return True
            elif "*" in pattern:
                # Wildcard pattern
                regex_pattern = pattern.replace(".", r"\.").replace("*", ".*")
                if re.match(regex_pattern, path) or re.match(regex_pattern, path_name):
                    return True
            # Exact match or path contains pattern
            elif pattern in path_parts or pattern in {path_name, path}:
                return True

        return False


class ApplicationBundler:
    """Bundle application files for transfer."""

    def __init__(self, config_path: str | None = None, verbose: bool = False) -> None:
        """Initialize the bundler.

        Args:
            config_path: Path to configuration file
            verbose: Enable verbose output

        """
        super().__init__()
        self.verbose: bool = verbose
        self.config: BundleConfig = self._load_config(config_path)
        self.gitignore_parser: GitIgnoreParser = GitIgnoreParser(".gitignore")

    def _load_config(self, config_path: str | None) -> BundleConfig:
        """Load configuration from file or use defaults.

        Args:
            config_path: Path to configuration file

        Returns:
            Configuration dictionary

        """
        default_config: BundleConfig = {
            "include_patterns": [
                "*.py",
                "*.toml",
                "*.json",
                "*.yml",
                "*.yaml",
                "*.ini",
                "*.cfg",
            ],
            "exclude_patterns": [
                "test_*.py",
                "*_test.py",
                "CLAUDE.md",
                "bundle_app.py",
                "transfer_cli.py",
                "decode_app.py",
                "transfer_config.json",
                "*.log",
                "*.tmp",
                "*.bak",
                "encoded_app_*.txt",
                "encoded_app_*.json",
            ],
            "exclude_dirs": [
                "tests",
                "test",
                "__pycache__",
                ".git",
                ".pytest_cache",
                ".ruff_cache",
                ".serena",
                "venv",
                "env",
                ".venv",
                "archive",
                "htmlcov",
                "encoded_releases",
                ".post-commit-output",
                "bundle_workflow_template",
                "deploy",
            ],
            "max_file_size_mb": 10,
            "chunk_size_kb": 5120,  # 5MB chunks
            "output_dir": "encoded_releases",
        }

        if config_path and Path(config_path).exists():
            try:
                with Path(config_path).open() as f:
                    user_config = cast("BundleConfig", json.load(f))
                    default_config.update(user_config)
                    if self.verbose:
                        print(f"Loaded config from {config_path}", file=sys.stderr)
            except Exception as e:
                print(
                    f"Warning: Failed to load config from {config_path}: {e}",
                    file=sys.stderr,
                )

        return default_config

    def should_include_file(self, file_path: str) -> bool:
        """Check if a file should be included in the bundle.

        Args:
            file_path: Relative path to the file

        Returns:
            True if file should be included

        """
        # Check gitignore patterns first
        if self.gitignore_parser.should_exclude(file_path):
            return False

        file_name = Path(file_path).name

        # Check exclude patterns from config
        exclude_patterns: list[str] = self.config.get("exclude_patterns", [])
        for pattern in exclude_patterns:
            if "*" in pattern:
                # Handle file extension patterns like *.log, *.pyc
                if pattern.startswith("*."):
                    extension = pattern[1:]  # Get .log, .pyc, etc.
                    if file_path.endswith(extension) or file_name.endswith(extension):
                        return False
                else:
                    # General wildcard pattern - anchor to start if pattern doesn't start with *
                    if pattern.startswith("*"):
                        regex_pattern = (
                            pattern.replace(".", r"\.").replace("*", ".*") + "$"
                        )
                    else:
                        regex_pattern = (
                            "^" + pattern.replace(".", r"\.").replace("*", ".*") + "$"
                        )
                    if re.search(regex_pattern, file_path) or re.search(
                        regex_pattern,
                        file_name,
                    ):
                        return False
            elif pattern in file_path or file_name == pattern:
                return False

        # Check include patterns
        include_patterns: list[str] = self.config.get("include_patterns", [])
        for pattern in include_patterns:
            if "*" in pattern:
                # Handle file extension patterns like *.py, *.sh
                if pattern.startswith("*."):
                    extension = pattern[1:]  # Get .py, .sh, etc.
                    if file_path.endswith(extension) or file_name.endswith(extension):
                        return True
                else:
                    # General wildcard pattern - anchor to start if pattern doesn't start with *
                    if pattern.startswith("*"):
                        regex_pattern = (
                            pattern.replace(".", r"\.").replace("*", ".*") + "$"
                        )
                    else:
                        regex_pattern = (
                            "^" + pattern.replace(".", r"\.").replace("*", ".*") + "$"
                        )
                    if re.search(regex_pattern, file_path) or re.search(
                        regex_pattern,
                        file_name,
                    ):
                        return True
            elif file_name == pattern:
                return True

        return False

    def collect_files(self, source_dir: str = ".") -> list[tuple[str, str]]:
        """Collect all files to be bundled.

        Args:
            source_dir: Source directory to scan

        Returns:
            List of (source_path, relative_path) tuples

        """
        files_to_bundle: list[tuple[str, str]] = []
        source_dir = str(Path(source_dir).resolve())
        max_size_bytes = self.config["max_file_size_mb"] * 1024 * 1024

        for root, dirs, files in os.walk(source_dir):
            # Filter out excluded directories
            dirs[:] = [
                d
                for d in dirs
                if d not in self.config["exclude_dirs"]
                and not self.gitignore_parser.should_exclude(d, is_dir=True)
            ]

            for file in files:
                file_path = str(Path(root) / file)
                relative_path = os.path.relpath(file_path, source_dir)

                # Skip files that are too large
                try:
                    file_path_obj = Path(file_path)
                    if file_path_obj.stat().st_size > max_size_bytes:
                        if self.verbose:
                            size_mb = file_path_obj.stat().st_size / (1024 * 1024)
                            print(
                                f"Skipping large file ({size_mb:.1f}MB): {relative_path}",
                                file=sys.stderr,
                            )
                        continue
                except OSError:
                    continue

                if self.should_include_file(relative_path):
                    files_to_bundle.append((file_path, relative_path))

        return files_to_bundle

    def create_bundle(self, output_dir: str | None = None) -> str:
        """Create a bundle of application files.

        Args:
            output_dir: Optional output directory (uses temp dir if not specified)

        Returns:
            Path to the bundle directory

        """
        # Collect files
        files_to_bundle = self.collect_files()

        if not files_to_bundle:
            msg = "No files found to bundle"
            raise ValueError(msg)

        if self.verbose:
            print(f"Found {len(files_to_bundle)} files to bundle", file=sys.stderr)

        # Create output directory
        if output_dir:
            bundle_dir = output_dir
            Path(bundle_dir).mkdir(parents=True, exist_ok=True)
        else:
            # Use a fixed temp directory name to avoid accumulation and race conditions
            bundle_dir_path = Path(tempfile.gettempdir()) / "search_script_bundle_temp"
            # Clean up any existing directory first
            if bundle_dir_path.exists():
                shutil.rmtree(bundle_dir_path)
            bundle_dir_path.mkdir(exist_ok=True)
            bundle_dir = str(bundle_dir_path)

        # Copy files to bundle directory
        for source_path, relative_path in files_to_bundle:
            dest_path = Path(bundle_dir) / relative_path
            dest_dir = dest_path.parent

            dest_dir.mkdir(parents=True, exist_ok=True)
            _ = shutil.copy2(source_path, dest_path)

            if self.verbose:
                print(f"Bundled: {relative_path}", file=sys.stderr)

        # Create bundle metadata
        metadata = {
            "created": datetime.now(tz=UTC).isoformat(),
            "files_count": len(files_to_bundle),
            "files": [rel_path for _, rel_path in files_to_bundle],
            "source_dir": str(Path.cwd()),
        }

        metadata_path = Path(bundle_dir) / ".bundle_metadata.json"
        with metadata_path.open("w") as f:
            json.dump(metadata, f, indent=2)

        return bundle_dir

    def encode_bundle(self, bundle_dir: str, output_file: str | None = None) -> str:
        """Encode the bundle using transfer_cli.py.

        Args:
            bundle_dir: Path to the bundle directory
            output_file: Optional output file path

        Returns:
            Path to the encoded file

        """
        if not output_file:
            timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
            output_file = f"encoded_app_{timestamp}.txt"

        # Build transfer_cli command
        transfer_cli_path = Path(__file__).parent / "transfer_cli.py"

        if not transfer_cli_path.exists():
            msg = f"transfer_cli.py not found at {transfer_cli_path}"
            raise FileNotFoundError(msg)

        cmd = [
            sys.executable,
            str(transfer_cli_path),
            bundle_dir,
            "-o",
            output_file,
            "-c",
            str(self.config["chunk_size_kb"]),
            "--single-file",
            "--metadata",
        ]

        if self.verbose:
            cmd.append("-v")
            print(f"Running: {' '.join(cmd)}", file=sys.stderr)

        # Run transfer_cli
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)

        if result.returncode != 0:
            msg = f"transfer_cli.py failed: {result.stderr}"
            raise RuntimeError(msg)

        if self.verbose and result.stderr:
            print(result.stderr, file=sys.stderr)

        return output_file


def main() -> None:
    """Main entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Bundle application files for base64 encoding",
    )
    _ = parser.add_argument(
        "-c",
        "--config",
        help="Configuration file path",
        default="bundle_workflow_template/transfer_config.json",
        type=str,
    )
    _ = parser.add_argument(
        "-o",
        "--output",
        help="Output file for encoded bundle",
        default=None,
        type=str,
    )
    _ = parser.add_argument(
        "--bundle-dir",
        help="Directory to create bundle in (temp dir if not specified)",
        default=None,
        type=str,
    )
    _ = parser.add_argument(
        "--keep-bundle",
        action="store_true",
        help="Keep the bundle directory after encoding",
    )
    _ = parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    _ = parser.add_argument(
        "--list-files",
        action="store_true",
        help="List files that would be bundled without creating bundle",
    )

    args = parser.parse_args()

    try:
        # Create bundler
        config_str = cast("str", args.config)
        verbose_bool = cast("bool", args.verbose)
        bundler = ApplicationBundler(
            config_path=config_str if Path(config_str).exists() else None,
            verbose=verbose_bool,
        )

        # List files mode
        list_files_bool = cast("bool", args.list_files)
        if list_files_bool:
            files = bundler.collect_files()
            print(f"Found {len(files)} files to bundle:")
            for source_path, relative_path in sorted(files, key=lambda x: x[1]):
                size_kb = Path(source_path).stat().st_size / 1024
                print(f"  {relative_path} ({size_kb:.1f} KB)")
            sys.exit(0)

        # Create bundle
        if verbose_bool:
            print("Creating application bundle...", file=sys.stderr)

        bundle_dir_arg = cast("str | None", args.bundle_dir)
        bundle_dir = bundler.create_bundle(bundle_dir_arg)

        if verbose_bool:
            print(f"Bundle created at: {bundle_dir}", file=sys.stderr)

        # Encode bundle
        if verbose_bool:
            print("Encoding bundle...", file=sys.stderr)

        output_arg = cast("str | None", args.output)
        output_file = bundler.encode_bundle(bundle_dir, output_arg)

        print(f"Encoded bundle saved to: {output_file}")

        # Clean up bundle directory if not keeping it
        keep_bundle_bool = cast("bool", args.keep_bundle)
        if not keep_bundle_bool and not bundle_dir_arg:
            shutil.rmtree(bundle_dir)
            if verbose_bool:
                print(f"Cleaned up bundle directory: {bundle_dir}", file=sys.stderr)

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
