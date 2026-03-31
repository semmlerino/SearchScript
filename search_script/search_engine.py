import fnmatch
import logging
import mmap
import os
import re
import types
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from rapidfuzz import fuzz

from .config import DirectoryError, FileAccessError, SearchError, ValidationError


@dataclass
class SearchResult:
    file_path: str
    line_number: int | None = None
    line_content: str | None = None
    next_line: str | None = None

    @property
    def display_text(self) -> str:
        if self.line_number and self.line_content:
            return f"{self.line_number}: {self.line_content}"
        return ""


class SearchMode(Enum):
    SUBSTRING = "substring"
    GLOB = "glob"
    REGEX = "regex"
    FUZZY = "fuzzy"


class SearchEngine:
    def __init__(self, logger: logging.Logger | None = None, max_workers: int = 4):
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self._binary_extensions = {
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".bin",
            ".obj",
            ".o",
            ".a",
            ".lib",
            ".zip",
            ".tar",
            ".gz",
            ".rar",
            ".7z",
            ".bz2",
            ".xz",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".bmp",
            ".tiff",
            ".svg",
            ".ico",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
            ".wmv",
            ".flv",
            ".mkv",
            ".webm",
            ".pdf",
            ".doc",
            ".docx",
            ".xls",
            ".xlsx",
            ".ppt",
            ".pptx",
            # VFX image formats
            ".exr",
            ".dpx",
            ".hdr",
            ".r3d",
            ".ari",
            ".braw",
            # VFX scene and cache formats
            ".abc",
            ".usd",
            ".usdc",
            ".bgeo",
            ".vdb",
            ".ma",
            ".mb",
            # Houdini formats
            ".hip",
            ".hipnc",
            # 3D interchange formats
            ".fbx",
            # Audio formats
            ".wav",
            ".aiff",
            # Color/LUT formats
            ".spi1d",
            ".spi3d",
            ".cube",
            # Nuke compiled format
            ".nknc",
        }
        try:
            import chardet  # pyright: ignore[reportMissingImports]

            self._chardet: types.ModuleType | None = chardet
        except ImportError:
            self._chardet = None

    def search_files(
        self,
        directory: str,
        search_term: str,
        include_types: list[str] | None = None,
        exclude_types: list[str] | None = None,
        search_within_files: bool = False,
        search_mode: SearchMode = SearchMode.SUBSTRING,
        max_depth: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
        match_folders: bool = False,
        progress_callback=None,
        cancel_event=None,
    ) -> Generator[SearchResult, None, None]:
        """
        Search for files/content matching criteria.

        Args:
            directory: Root directory to search
            search_term: Term to search for
            include_types: File extensions to include (e.g., ['.txt', '.py'])
            exclude_types: File extensions to exclude
            search_within_files: If True, search file contents; if False, search filenames
            progress_callback: Callback function for progress updates
            cancel_event: Threading event to signal cancellation

        Yields:
            SearchResult objects for each match found

        Raises:
            DirectoryError: If directory doesn't exist or isn't accessible
            ValidationError: If search parameters are invalid
            SearchError: For other search-related errors
        """
        # Validate inputs
        self._validate_search_params(directory, search_term)

        include_types = [ext.lower() for ext in (include_types or [])]
        exclude_types = [ext.lower() for ext in (exclude_types or [])]
        files_processed = 0

        self.logger.debug(f"Starting search in directory: {directory}")

        root_depth = directory.rstrip(os.sep).count(os.sep)
        try:
            for root_dir, dirs, files in os.walk(directory):
                current_depth = root_dir.rstrip(os.sep).count(os.sep) - root_depth
                if max_depth is not None and current_depth >= max_depth:
                    dirs.clear()  # Don't descend further
                    continue

                if match_folders and not search_within_files:
                    folder_name = os.path.basename(root_dir)
                    if self._matches_term(folder_name, search_term, search_mode):
                        yield SearchResult(root_dir)

                for file in files:
                    if cancel_event and cancel_event.is_set():
                        self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                        return

                    file_path = Path(root_dir) / file
                    file_lower = file.lower()

                    # Apply file type filters
                    if not self._should_process_file(
                        file_lower,
                        include_types,
                        exclude_types,
                        search_within_files=search_within_files,
                    ):
                        files_processed += 1
                        self._update_progress(files_processed, progress_callback)
                        continue

                    # Apply size/date filters
                    if not self._check_file_filters(
                        file_path, min_size, max_size, modified_after, modified_before
                    ):
                        files_processed += 1
                        self._update_progress(files_processed, progress_callback)
                        continue

                    # Perform search
                    try:
                        if search_within_files:
                            yield from self._search_file_content(
                                file_path, search_term, search_mode
                            )
                        else:
                            if self._matches_term(file, search_term, search_mode):
                                yield SearchResult(str(file_path))
                    except FileAccessError as e:
                        self.logger.warning(f"Skipping file due to access error: {e}")
                        continue

                    files_processed += 1
                    self._update_progress(files_processed, progress_callback)

        except PermissionError as e:
            raise DirectoryError(f"Permission denied accessing directory: {directory}") from e
        except FileNotFoundError as e:
            raise DirectoryError(f"Directory not found: {directory}") from e
        except Exception as e:
            self.logger.error(f"Unexpected error during search: {e}")
            raise SearchError(f"Search operation failed: {e!s}") from e

    def _validate_search_params(self, directory: str, search_term: str):
        """Validate search parameters."""
        if not directory or not directory.strip():
            raise ValidationError("Directory path cannot be empty")

        if not os.path.exists(directory):
            raise DirectoryError(f"Directory does not exist: {directory}")

        if not os.path.isdir(directory):
            raise DirectoryError(f"Path is not a directory: {directory}")

        if not search_term or not search_term.strip():
            raise ValidationError("Search term cannot be empty")

    def _should_process_file(
        self,
        file_lower: str,
        include_types: list[str],
        exclude_types: list[str],
        search_within_files: bool = True,
    ) -> bool:
        """Check if file should be processed based on type filters."""
        # Skip known binary file types for content search only
        file_ext = Path(file_lower).suffix.lower()
        if search_within_files and file_ext in self._binary_extensions:
            return False

        if include_types and not any(file_lower.endswith(ext) for ext in include_types):
            return False
        return not (exclude_types and any(file_lower.endswith(ext) for ext in exclude_types))

    def _matches_term(self, text: str, search_term: str, mode: SearchMode) -> bool:
        """Match text against search_term using the specified mode."""
        if mode == SearchMode.SUBSTRING:
            return search_term.lower() in text.lower()
        elif mode == SearchMode.GLOB:
            has_glob_chars = any(c in search_term for c in "*?[]")
            pattern = search_term if has_glob_chars else f"*{search_term}*"
            return fnmatch.fnmatch(text.lower(), pattern.lower())
        elif mode == SearchMode.REGEX:
            try:
                return bool(re.search(search_term, text, re.IGNORECASE))
            except re.error:
                return False
        elif mode == SearchMode.FUZZY:
            return fuzz.partial_ratio(search_term.lower(), text.lower()) >= 70
        return False

    def _check_file_filters(
        self,
        file_path: Path,
        min_size: int | None,
        max_size: int | None,
        modified_after: datetime | None,
        modified_before: datetime | None,
    ) -> bool:
        """Return True if the file passes all size/date filters."""
        try:
            stat = file_path.stat()
            if min_size is not None and stat.st_size < min_size:
                return False
            if max_size is not None and stat.st_size > max_size:
                return False
            if modified_after is not None or modified_before is not None:
                mod_time = datetime.fromtimestamp(stat.st_mtime)
                if modified_after and mod_time < modified_after:
                    return False
                if modified_before and mod_time > modified_before:
                    return False
        except OSError:
            return False
        return True

    def _detect_encoding(self, file_path: Path) -> str:
        """Detect file encoding using chardet if available, otherwise fall back to utf-8."""
        if self._chardet is None:
            return "utf-8"
        try:
            with open(file_path, "rb") as f:
                raw = f.read(4096)
            detected = self._chardet.detect(raw)
            if detected and detected.get("confidence", 0) > 0.5:
                return detected["encoding"] or "utf-8"
        except Exception:
            pass
        return "utf-8"

    def _search_file_content(
        self, file_path: Path, search_term: str, search_mode: SearchMode = SearchMode.SUBSTRING
    ) -> Generator[SearchResult, None, None]:
        """Search within file content for the search term using optimized methods."""
        file_size = file_path.stat().st_size

        # Skip empty files
        if file_size == 0:
            return

        # Use memory mapping for large files (>1MB)
        if file_size > 1024 * 1024:
            yield from self._search_large_file(file_path, search_term, search_mode)
        else:
            yield from self._search_small_file(file_path, search_term, search_mode)

    def _search_small_file(
        self, file_path: Path, search_term: str, search_mode: SearchMode = SearchMode.SUBSTRING
    ) -> Generator[SearchResult, None, None]:
        """Search small files using standard file reading."""
        try:
            encoding = self._detect_encoding(file_path)
            with open(file_path, encoding=encoding, errors="ignore") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if self._matches_term(line, search_term, search_mode):
                    line_content = line.strip()
                    if len(line_content) > 2000:
                        line_content = line_content[:2000] + "..."
                    next_line = lines[i + 1].strip() if i + 1 < len(lines) else None
                    if next_line and len(next_line) > 2000:
                        next_line = next_line[:2000] + "..."
                    yield SearchResult(str(file_path), i + 1, line_content, next_line)
        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except Exception as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _search_large_file(
        self, file_path: Path, search_term: str, search_mode: SearchMode = SearchMode.SUBSTRING
    ) -> Generator[SearchResult, None, None]:
        """Search large files using memory mapping for better performance."""
        try:
            with open(file_path, "rb") as f:
                try:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        search_bytes = search_term.lower().encode("utf-8", errors="ignore")

                        # Find all occurrences
                        start = 0
                        while True:
                            pos = mm.find(search_bytes, start)
                            if pos == -1:
                                break

                            # Find line boundaries
                            line_start = mm.rfind(b"\n", 0, pos) + 1
                            line_end = mm.find(b"\n", pos)
                            if line_end == -1:
                                line_end = len(mm)

                            # Get line content and number
                            line_content = (
                                mm[line_start:line_end].decode("utf-8", errors="ignore").strip()
                            )
                            line_num = mm[:line_start].count(b"\n") + 1

                            # Truncate long lines
                            if len(line_content) > 2000:
                                line_content = line_content[:2000] + "..."

                            yield SearchResult(str(file_path), line_num, line_content)

                            start = pos + 1

                except (OSError, ValueError):
                    # Fallback to regular file reading if mmap fails
                    yield from self._search_small_file(file_path, search_term, search_mode)

        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except Exception as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _update_progress(self, files_processed: int, progress_callback):
        """Update progress if callback provided."""
        if progress_callback and files_processed % 10 == 0:
            progress_callback(f"Files processed: {files_processed}")
