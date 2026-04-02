#!/usr/bin/env python3
"""Bundle application files for base64 encoding.

This script collects all relevant application files (respecting .gitignore),
copies them to a temporary directory, and encodes them using transfer_cli.py.
"""

from __future__ import annotations

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
        with Path(gitignore_path).open() as f:
            for line in f:
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith("#"):
                    self.patterns.append(stripped_line)

    def should_exclude(self, path: str, is_dir: bool = False) -> bool:
        path_parts = Path(path).parts
        path_name = Path(path).name

        for pattern in self.always_exclude:
            if pattern.startswith("*."):
                extension = pattern[1:]
                if path.endswith(extension) or path_name.endswith(extension):
                    return True
            elif pattern in path_parts or path_name == pattern:
                return True

        for pattern in self.patterns:
            if pattern.endswith("/"):
                if is_dir and (pattern[:-1] in path_parts or path_name == pattern[:-1]):
                    return True
            elif "*" in pattern:
                regex_pattern = pattern.replace(".", r"\.").replace("*", ".*")
                if re.match(regex_pattern, path) or re.match(regex_pattern, path_name):
                    return True
            elif pattern in path_parts or pattern in {path_name, path}:
                return True

        return False


class ApplicationBundler:
    """Bundle application files for transfer."""

    def __init__(self, verbose: bool = False) -> None:
        super().__init__()
        self.verbose: bool = verbose
        self.config: BundleConfig = self._default_config()
        self.gitignore_parser: GitIgnoreParser = GitIgnoreParser(".gitignore")

    def _default_config(self) -> BundleConfig:
        return {
            "include_patterns": [
                "*.py",
                "*.toml",
                "*.json",
                "*.yml",
                "*.yaml",
                "*.md",
                "*.txt",
                "*.ini",
                "*.cfg",
            ],
            "exclude_patterns": [
                "test_*.py",
                "*_test.py",
                "bundle_app.py",
                "transfer_cli.py",
                "decode_app.py",
                "CLAUDE.md",
                "*.log",
                "*.tmp",
                "*.bak",
                "encoded_app_*.txt",
            ],
            "exclude_dirs": [
                "tests",
                "test",
                "__pycache__",
                ".git",
                ".pytest_cache",
                ".serena",
                "venv",
                "env",
                ".venv",
                "archive",
                "htmlcov",
                "encoded_releases",
                "deploy",
            ],
            "max_file_size_mb": 10,
            "chunk_size_kb": 5120,
            "output_dir": "encoded_releases",
        }

    def should_include_file(self, file_path: str) -> bool:
        if self.gitignore_parser.should_exclude(file_path):
            return False

        file_name = Path(file_path).name

        exclude_patterns: list[str] = self.config.get("exclude_patterns", [])
        for pattern in exclude_patterns:
            if "*" in pattern:
                if pattern.startswith("*."):
                    extension = pattern[1:]
                    if file_path.endswith(extension) or file_name.endswith(extension):
                        return False
                else:
                    if pattern.startswith("*"):
                        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*") + "$"
                    else:
                        regex_pattern = "^" + pattern.replace(".", r"\.").replace("*", ".*") + "$"
                    if re.search(regex_pattern, file_path) or re.search(regex_pattern, file_name):
                        return False
            elif pattern in file_path or file_name == pattern:
                return False

        include_patterns: list[str] = self.config.get("include_patterns", [])
        for pattern in include_patterns:
            if "*" in pattern:
                if pattern.startswith("*."):
                    extension = pattern[1:]
                    if file_path.endswith(extension) or file_name.endswith(extension):
                        return True
                else:
                    if pattern.startswith("*"):
                        regex_pattern = pattern.replace(".", r"\.").replace("*", ".*") + "$"
                    else:
                        regex_pattern = "^" + pattern.replace(".", r"\.").replace("*", ".*") + "$"
                    if re.search(regex_pattern, file_path) or re.search(regex_pattern, file_name):
                        return True
            elif file_name == pattern:
                return True

        return False

    def collect_files(self, source_dir: str = ".") -> list[tuple[str, str]]:
        files_to_bundle: list[tuple[str, str]] = []
        source_dir = str(Path(source_dir).resolve())
        max_size_bytes = self.config["max_file_size_mb"] * 1024 * 1024

        for root, dirs, files in os.walk(source_dir):
            dirs[:] = [
                d
                for d in dirs
                if d not in self.config["exclude_dirs"]
                and not self.gitignore_parser.should_exclude(d, is_dir=True)
            ]

            for file in files:
                file_path = str(Path(root) / file)
                relative_path = os.path.relpath(file_path, source_dir)

                try:
                    if Path(file_path).stat().st_size > max_size_bytes:
                        if self.verbose:
                            size_mb = Path(file_path).stat().st_size / (1024 * 1024)
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
        files_to_bundle = self.collect_files()

        if not files_to_bundle:
            msg = "No files found to bundle"
            raise ValueError(msg)

        if self.verbose:
            print(f"Found {len(files_to_bundle)} files to bundle", file=sys.stderr)

        if output_dir:
            bundle_dir = output_dir
            Path(bundle_dir).mkdir(parents=True, exist_ok=True)
        else:
            bundle_dir_path = Path(tempfile.gettempdir()) / "search_script_bundle_temp"
            if bundle_dir_path.exists():
                shutil.rmtree(bundle_dir_path)
            bundle_dir_path.mkdir(exist_ok=True)
            bundle_dir = str(bundle_dir_path)

        for source_path, relative_path in files_to_bundle:
            dest_path = Path(bundle_dir) / relative_path
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            _ = shutil.copy2(source_path, dest_path)

            if self.verbose:
                print(f"Bundled: {relative_path}", file=sys.stderr)

        metadata = {
            "created": datetime.now(tz=UTC).isoformat(),
            "files_count": len(files_to_bundle),
            "files": [rel_path for _, rel_path in files_to_bundle],
            "source_dir": str(Path.cwd()),
        }

        with (Path(bundle_dir) / ".bundle_metadata.json").open("w") as f:
            json.dump(metadata, f, indent=2)

        return bundle_dir

    def encode_bundle(self, bundle_dir: str, output_file: str | None = None) -> str:
        if not output_file:
            timestamp = datetime.now(tz=UTC).strftime("%Y%m%d_%H%M%S")
            output_file = str(
                Path(self.config["output_dir"]) / f"encoded_app_{timestamp}.txt"
            )

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
        description="Bundle search-script files for base64 encoding",
    )
    _ = parser.add_argument(
        "-o",
        "--output",
        help="Output file for encoded bundle (default: encoded_releases/encoded_app_<timestamp>.txt)",
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
    verbose_bool = cast("bool", args.verbose)
    bundler = ApplicationBundler(verbose=verbose_bool)

    if cast("bool", args.list_files):
        files = bundler.collect_files()
        print(f"Found {len(files)} files to bundle:")
        for source_path, relative_path in sorted(files, key=lambda x: x[1]):
            size_kb = Path(source_path).stat().st_size / 1024
            print(f"  {relative_path} ({size_kb:.1f} KB)")
        sys.exit(0)

    try:
        Path(bundler.config["output_dir"]).mkdir(parents=True, exist_ok=True)

        bundle_dir_arg = cast("str | None", args.bundle_dir)
        bundle_dir = bundler.create_bundle(bundle_dir_arg)

        if verbose_bool:
            print(f"Bundle created at: {bundle_dir}", file=sys.stderr)

        output_arg = cast("str | None", args.output)
        output_file = bundler.encode_bundle(bundle_dir, output_arg)

        print(f"Encoded bundle saved to: {output_file}")

        keep_bundle_bool = cast("bool", args.keep_bundle)
        if not keep_bundle_bool and not bundle_dir_arg:
            shutil.rmtree(bundle_dir)
            if verbose_bool:
                print(f"Cleaned up bundle directory: {bundle_dir}", file=sys.stderr)

    except Exception as e:  # noqa: BLE001
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
