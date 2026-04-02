import fnmatch
import json
import logging
import mmap
import os
import re
import shutil
import subprocess
import threading
import types
from base64 import b64decode
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from queue import Empty, Queue

try:
    from rapidfuzz import fuzz

    RAPIDFUZZ_AVAILABLE = True
except ImportError:
    RAPIDFUZZ_AVAILABLE = False

from .config import DirectoryError, FileAccessError, SearchError, ValidationError


@dataclass
class SearchResult:
    file_path: str
    line_number: int | None = None
    line_content: str | None = None
    next_line: str | None = None
    mod_time: float | None = None
    file_size: int | None = None

    @property
    def display_text(self) -> str:
        if self.line_number and self.line_content:
            return f"{self.line_number}: {self.line_content}"
        return ""

    @property
    def formatted_mod_time(self) -> str:
        if self.mod_time is not None:
            return datetime.fromtimestamp(self.mod_time).strftime("%Y-%m-%d %H:%M:%S")
        return "N/A"

    @property
    def formatted_size(self) -> str:
        if self.file_size is not None:
            size = float(self.file_size)
            for unit in ["B", "KB", "MB", "GB", "TB"]:
                if size < 1024.0:
                    return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} {unit}"
                size /= 1024.0
            return f"{size:.1f} PB"
        return "N/A"


class SearchMode(Enum):
    SUBSTRING = "substring"
    GLOB = "glob"
    REGEX = "regex"
    FUZZY = "fuzzy"


class SearchBackend(Enum):
    AUTO = "auto"
    PYTHON = "python"
    RIPGREP = "ripgrep"


class SearchEngine:
    def __init__(self, logger: logging.Logger | None = None, max_workers: int = 4):
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self._rg_path = shutil.which("rg")
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
            ".mb",
            # Houdini formats
            ".hip",
            ".hipnc",
            # 3D interchange formats
            ".fbx",
            # Audio formats
            ".wav",
            ".aiff",
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
        search_backend: SearchBackend = SearchBackend.AUTO,
        max_depth: int | None = None,
        min_size: int | None = None,
        max_size: int | None = None,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
        match_folders: bool = False,
        follow_symlinks: bool = False,
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
            follow_symlinks: If True, follow symbolic links during traversal
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

        if search_mode == SearchMode.REGEX:
            try:
                re.compile(search_term)
            except re.error:
                self.logger.debug("Skipping invalid regex search term")
                return

        backend = self._resolve_search_backend(search_within_files, search_mode, search_backend)
        if backend == SearchBackend.RIPGREP:
            yield from self._search_file_content_with_ripgrep(
                directory=directory,
                search_term=search_term,
                search_mode=search_mode,
                include_types=include_types,
                exclude_types=exclude_types,
                max_depth=max_depth,
                min_size=min_size,
                max_size=max_size,
                modified_after=modified_after,
                modified_before=modified_before,
                follow_symlinks=follow_symlinks,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
            )
            return

        files_processed = 0

        self.logger.debug(f"Starting search in directory: {directory}")

        try:
            for dir_path, entry in self._walk_scandir(
                directory, max_depth, follow_symlinks, cancel_event
            ):
                if cancel_event and cancel_event.is_set():
                    self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                    return

                # Directory notification — check folder name match
                if entry is None:
                    if match_folders and not search_within_files:
                        folder_name = os.path.basename(dir_path)
                        if self._matches_term(folder_name, search_term, search_mode):
                            yield SearchResult(dir_path)
                    continue

                file_name = entry.name
                file_lower = file_name.lower()

                # Apply file type filters
                if not self._should_process_file(
                    file_lower,
                    include_types,
                    exclude_types,
                    search_within_files=search_within_files,
                    file_path=Path(entry.path),
                ):
                    files_processed += 1
                    self._update_progress(files_processed, dir_path, progress_callback)
                    continue

                # Single stat call per file
                try:
                    entry_stat = entry.stat(follow_symlinks=follow_symlinks)
                except OSError:
                    files_processed += 1
                    self._update_progress(files_processed, dir_path, progress_callback)
                    continue

                # Apply size/date filters using the stat we already have
                if not self._check_file_filters_with_stat(
                    entry_stat, min_size, max_size, modified_after, modified_before
                ):
                    files_processed += 1
                    self._update_progress(files_processed, dir_path, progress_callback)
                    continue

                file_path = Path(entry.path)

                # Perform search
                try:
                    if search_within_files:
                        for result in self._search_file_content(
                            file_path, search_term, search_mode, file_size=entry_stat.st_size
                        ):
                            result.mod_time = entry_stat.st_mtime
                            yield result
                    else:
                        if self._matches_term(file_name, search_term, search_mode):
                            yield SearchResult(
                                str(file_path),
                                mod_time=entry_stat.st_mtime,
                                file_size=entry_stat.st_size,
                            )
                except FileAccessError as e:
                    self.logger.warning(f"Skipping file due to access error: {e}")
                    continue

                files_processed += 1
                self._update_progress(files_processed, dir_path, progress_callback)

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

    def _is_likely_binary(self, file_path: Path) -> bool:
        """Return True if the file appears to be binary (contains null bytes in the first 8 KB)."""
        try:
            with open(file_path, "rb") as f:
                return b"\x00" in f.read(8192)
        except OSError:
            return True

    def _should_process_file(
        self,
        file_lower: str,
        include_types: list[str],
        exclude_types: list[str],
        search_within_files: bool = True,
        file_path: Path | None = None,
    ) -> bool:
        """Check if file should be processed based on type filters."""
        # Skip known binary file types for content search only; allow through if
        # the file doesn't actually contain null bytes (e.g. text-based USD/USDA).
        file_ext = Path(file_lower).suffix.lower()
        if (
            search_within_files
            and file_ext in self._binary_extensions
            and (file_path is None or self._is_likely_binary(file_path))
        ):
            return False

        if include_types and not any(file_lower.endswith(ext) for ext in include_types):
            return False
        return not (exclude_types and any(file_lower.endswith(ext) for ext in exclude_types))

    def _resolve_search_backend(
        self,
        search_within_files: bool,
        search_mode: SearchMode,
        requested_backend: SearchBackend,
    ) -> SearchBackend:
        """Select the concrete backend while preserving Python fallback semantics."""
        if not search_within_files or search_mode == SearchMode.FUZZY:
            return SearchBackend.PYTHON
        if requested_backend == SearchBackend.PYTHON:
            return SearchBackend.PYTHON
        if self._rg_path is None:
            return SearchBackend.PYTHON
        return SearchBackend.RIPGREP

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
            if not RAPIDFUZZ_AVAILABLE:
                return False
            return fuzz.partial_ratio(search_term.lower(), text.lower()) >= 70  # pyright: ignore[reportPossiblyUnboundVariable]
        return False

    def _check_file_filters_with_stat(
        self,
        stat_result: os.stat_result,
        min_size: int | None,
        max_size: int | None,
        modified_after: datetime | None,
        modified_before: datetime | None,
    ) -> bool:
        """Return True if the file passes all size/date filters (using pre-fetched stat)."""
        if min_size is not None and stat_result.st_size < min_size:
            return False
        if max_size is not None and stat_result.st_size > max_size:
            return False
        if modified_after is not None or modified_before is not None:
            mod_time = datetime.fromtimestamp(stat_result.st_mtime)
            if modified_after and mod_time < modified_after:
                return False
            if modified_before and mod_time > modified_before:
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
        self,
        file_path: Path,
        search_term: str,
        search_mode: SearchMode = SearchMode.SUBSTRING,
        file_size: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search within file content for the search term using optimized methods."""
        if file_size == 0:
            return
        if file_size > 1024 * 1024:
            yield from self._search_large_file(
                file_path, search_term, search_mode, file_size=file_size
            )
        else:
            yield from self._search_small_file(
                file_path, search_term, search_mode, file_size=file_size
            )

    def _search_small_file(
        self,
        file_path: Path,
        search_term: str,
        search_mode: SearchMode = SearchMode.SUBSTRING,
        file_size: int | None = None,
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
                    yield SearchResult(
                        str(file_path), i + 1, line_content, next_line, file_size=file_size
                    )
        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except Exception as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _search_file_content_with_ripgrep(
        self,
        directory: str,
        search_term: str,
        search_mode: SearchMode,
        include_types: list[str],
        exclude_types: list[str],
        max_depth: int | None,
        min_size: int | None,
        max_size: int | None,
        modified_after: datetime | None,
        modified_before: datetime | None,
        follow_symlinks: bool,
        progress_callback=None,
        cancel_event: threading.Event | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Search file contents with ripgrep and filter matches using the app's metadata rules."""
        if self._rg_path is None:
            return

        command = self._build_ripgrep_command(
            directory=directory,
            search_term=search_term,
            search_mode=search_mode,
            include_types=include_types,
            exclude_types=exclude_types,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
        )
        if progress_callback:
            progress_callback(f"Searching with ripgrep: {directory}")

        try:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
        except OSError as e:
            self.logger.warning(f"ripgrep backend unavailable, falling back to Python search: {e}")
            yield from self._search_file_content_with_python_walk(
                directory=directory,
                search_term=search_term,
                include_types=include_types,
                exclude_types=exclude_types,
                search_mode=search_mode,
                max_depth=max_depth,
                min_size=min_size,
                max_size=max_size,
                modified_after=modified_after,
                modified_before=modified_before,
                follow_symlinks=follow_symlinks,
                progress_callback=progress_callback,
                cancel_event=cancel_event,
            )
            return

        assert process.stdout is not None
        stdout = process.stdout
        stdout_queue: Queue[str | None] = Queue()

        def _reader() -> None:
            for raw_line in stdout:
                stdout_queue.put(raw_line)
            stdout_queue.put(None)

        threading.Thread(target=_reader, daemon=True).start()
        search_root = Path(directory)
        stat_cache: dict[str, os.stat_result] = {}
        matches = 0

        try:
            while True:
                if cancel_event and cancel_event.is_set():
                    self._terminate_process(process)
                    return

                try:
                    raw_line = stdout_queue.get(timeout=0.1)
                except Empty:
                    if process.poll() is not None:
                        break
                    continue

                if raw_line is None:
                    break

                try:
                    payload = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                if payload.get("type") != "match":
                    continue

                data = payload["data"]
                file_path = self._resolve_ripgrep_path(search_root, data["path"])
                cache_key = str(file_path)
                stat_result = stat_cache.get(cache_key)
                if stat_result is None:
                    try:
                        stat_result = file_path.stat(follow_symlinks=follow_symlinks)
                    except OSError:
                        continue
                    stat_cache[cache_key] = stat_result

                if not self._check_file_filters_with_stat(
                    stat_result, min_size, max_size, modified_after, modified_before
                ):
                    continue

                line_text = self._decode_ripgrep_text(data["lines"]).rstrip("\n").rstrip("\r")
                if len(line_text) > 2000:
                    line_text = line_text[:2000] + "..."

                matches += 1
                if progress_callback and matches % 1000 == 0:
                    progress_callback(f"ripgrep matched {matches} lines in {directory}")

                yield SearchResult(
                    str(file_path),
                    data.get("line_number"),
                    line_text.strip(),
                    file_size=stat_result.st_size,
                    mod_time=stat_result.st_mtime,
                )

            return_code = process.wait()
            if return_code not in (0, 1):
                stderr = ""
                if process.stderr is not None:
                    stderr = process.stderr.read().strip()
                raise SearchError(stderr or f"ripgrep search failed with exit code {return_code}")
        finally:
            self._terminate_process(process)

    def _search_file_content_with_python_walk(
        self,
        directory: str,
        search_term: str,
        include_types: list[str],
        exclude_types: list[str],
        search_mode: SearchMode,
        max_depth: int | None,
        min_size: int | None,
        max_size: int | None,
        modified_after: datetime | None,
        modified_before: datetime | None,
        follow_symlinks: bool,
        progress_callback=None,
        cancel_event: threading.Event | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Fallback helper that preserves existing Python traversal semantics."""
        yield from self.search_files(
            directory=directory,
            search_term=search_term,
            include_types=include_types,
            exclude_types=exclude_types,
            search_within_files=True,
            search_mode=search_mode,
            search_backend=SearchBackend.PYTHON,
            max_depth=max_depth,
            min_size=min_size,
            max_size=max_size,
            modified_after=modified_after,
            modified_before=modified_before,
            follow_symlinks=follow_symlinks,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
        )

    def _build_ripgrep_command(
        self,
        directory: str,
        search_term: str,
        search_mode: SearchMode,
        include_types: list[str],
        exclude_types: list[str],
        max_depth: int | None,
        follow_symlinks: bool,
    ) -> list[str]:
        """Build a ripgrep command that preserves this app's file-selection semantics."""
        if self._rg_path is None:
            raise SearchError("ripgrep is not available")

        command = [
            self._rg_path,
            "--json",
            "--line-number",
            "--color",
            "never",
            "--no-messages",
            "--glob-case-insensitive",
            "--threads",
            str(self.max_workers),
            "-uu",
        ]
        if follow_symlinks:
            command.append("-L")
        if max_depth is not None:
            command.extend(["--max-depth", str(max_depth)])
        for ext in include_types:
            command.extend(["-g", f"*{ext}"])
        for ext in exclude_types:
            command.extend(["-g", f"!*{ext}"])

        pattern = search_term
        if search_mode == SearchMode.SUBSTRING:
            command.append("--fixed-strings")
        elif search_mode == SearchMode.GLOB:
            pattern = self._translate_glob_to_regex(search_term)

        command.extend(["-i", pattern, directory])
        return command

    def _translate_glob_to_regex(self, search_term: str) -> str:
        """Translate the app's line-glob semantics into a ripgrep-compatible regex."""
        pattern = search_term if any(c in search_term for c in "*?[]") else f"*{search_term}*"
        translated = fnmatch.translate(pattern)
        if translated.startswith("(?s:") and translated.endswith(")\\Z"):
            translated = translated[4:-3]
        elif translated.endswith("\\Z"):
            translated = translated[:-2]
        return f"^{translated}$"

    def _resolve_ripgrep_path(self, search_root: Path, path_info: dict[str, str]) -> Path:
        """Resolve ripgrep's match path into an absolute path under the searched root."""
        raw_path = self._decode_ripgrep_text(path_info)
        path = Path(raw_path)
        if path.is_absolute():
            return path
        return search_root / path

    def _decode_ripgrep_text(self, payload: dict[str, str]) -> str:
        """Decode a ripgrep JSON text payload that may contain plain text or base64 bytes."""
        if "text" in payload:
            return payload["text"]
        if "bytes" in payload:
            return b64decode(payload["bytes"]).decode("utf-8", errors="replace")
        return ""

    def _terminate_process(self, process: subprocess.Popen[str]) -> None:
        """Terminate a subprocess if it is still running."""
        if process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()

    def _search_large_file(
        self,
        file_path: Path,
        search_term: str,
        search_mode: SearchMode = SearchMode.SUBSTRING,
        file_size: int | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Search large files using memory mapping for better performance."""
        try:
            with open(file_path, "rb") as f:
                try:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        encoding = self._detect_encoding(file_path)
                        line_no = 0
                        pos = 0
                        while pos < len(mm):
                            eol = mm.find(b"\n", pos)
                            end = eol if eol != -1 else len(mm)
                            line_bytes = mm[pos:end]
                            line_text = line_bytes.decode(encoding, errors="replace").rstrip("\r")
                            line_no += 1
                            if self._matches_term(line_text, search_term, search_mode):
                                line_content = line_text.strip()
                                if len(line_content) > 2000:
                                    line_content = line_content[:2000] + "..."
                                # Peek at next line for context_after
                                next_line: str | None = None
                                if eol != -1:
                                    next_eol = mm.find(b"\n", end + 1)
                                    next_end = next_eol if next_eol != -1 else len(mm)
                                    raw_next = (
                                        mm[end + 1 : next_end]
                                        .decode(encoding, errors="replace")
                                        .rstrip("\r")
                                        .strip()
                                    )
                                    if raw_next:
                                        if len(raw_next) > 2000:
                                            raw_next = raw_next[:2000] + "..."
                                        next_line = raw_next
                                yield SearchResult(
                                    str(file_path),
                                    line_no,
                                    line_content,
                                    next_line,
                                    file_size=file_size,
                                )
                            pos = end + 1

                except (OSError, ValueError):
                    # Fallback to regular file reading if mmap fails
                    yield from self._search_small_file(
                        file_path, search_term, search_mode, file_size=file_size
                    )

        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except Exception as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _update_progress(self, files_processed: int, current_dir: str, progress_callback):
        """Update progress if callback provided."""
        if progress_callback and files_processed % 10 == 0:
            progress_callback(f"Scanning: {current_dir} ({files_processed} files)")

    def _walk_scandir(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        cancel_event: threading.Event | None,
        _current_depth: int = 0,
        _seen_realpaths: set[str] | None = None,
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

        # Yield directory notification (entry=None signals "entering directory")
        yield (directory, None)

        try:
            with os.scandir(directory) as entries:
                subdirs: list[os.DirEntry[str]] = []
                for entry in entries:
                    if cancel_event and cancel_event.is_set():
                        return
                    try:
                        if entry.is_file(follow_symlinks=follow_symlinks):
                            yield (directory, entry)
                        elif entry.is_dir(follow_symlinks=follow_symlinks):
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
                _current_depth + 1,
                _seen_realpaths,
            )
