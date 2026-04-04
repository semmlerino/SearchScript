import codecs
import concurrent.futures
import fnmatch
import logging
import mmap
import os
import re
import shutil
import threading
import types
from collections import deque
from collections.abc import Callable, Generator, Sequence
from datetime import datetime
from pathlib import Path

try:
    from rapidfuzz import fuzz
except ImportError:
    fuzz = None  # type: ignore[assignment]  # pyright: ignore[reportConstantRedefinition]

from .config import DirectoryError, FileAccessError, SearchError, ValidationError
from .constants import (
    CONTENT_SEARCH_POOL_CHUNK_SIZE,
    FUZZY_EXACT_BONUS,
    FUZZY_FULL_THRESHOLD,
    FUZZY_PARTIAL_THRESHOLD,
    FUZZY_WORD_BONUS,
    LARGE_FILE_MMAP_THRESHOLD,
)
from .inventory import InventoryManager
from .models import (
    RAPIDFUZZ_AVAILABLE,
    MatchPlan,
    SearchBackend,
    SearchMode,
    SearchResult,
    check_file_filters,
    ensure_glob_wildcard,
    truncate_line,
)
from .ripgrep_backend import RipgrepBackend, RipgrepUnavailableError
from .search_index import (
    InventoryEntry,
)


def _collect_context(
    lines: Sequence[str],
    match_index: int,
    context_lines: int,
) -> tuple[list[str] | None, list[str] | None, str | None]:
    """Collect before/after context lines and next_line for a matched line.

    Returns (ctx_before, ctx_after, next_line_override).
    next_line_override is the first after-context line when available, else None.
    """
    if context_lines <= 0:
        return None, None, None
    before_start = max(0, match_index - context_lines)
    ctx_before = [truncate_line(ln.strip()) for ln in lines[before_start:match_index]]
    after_end = min(len(lines), match_index + 1 + context_lines)
    ctx_after = [truncate_line(ln.strip()) for ln in lines[match_index + 1 : after_end]]
    next_line_override = ctx_after[0] if ctx_after else None
    return ctx_before, ctx_after, next_line_override


class SearchEngine:
    def __init__(
        self,
        logger: logging.Logger | None = None,
        max_workers: int = 4,
        index_db_path: str | Path | None = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.max_workers = max_workers
        self._rg_path = shutil.which("rg")
        self._inventory = InventoryManager(self.logger, index_db_path=index_db_path)
        self._ripgrep = RipgrepBackend(self.logger, self._rg_path, max_workers=self.max_workers)
        self._always_binary_extensions = {
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
            ".usdc",
            ".usdz",
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
        self._maybe_binary_extensions = {".usd"}
        try:
            import chardet  # pyright: ignore[reportMissingImports]

            self._chardet: types.ModuleType | None = chardet
        except ImportError:
            self._chardet = None

    def shutdown(self) -> None:
        """Signal background threads to stop."""
        self._inventory.shutdown()

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
        max_results: int | None = None,
        modified_after: datetime | None = None,
        modified_before: datetime | None = None,
        match_folders: bool = False,
        follow_symlinks: bool = False,
        include_ignored: bool = True,
        context_lines: int = 0,
        case_sensitive: bool = False,
        exclude_shots: bool = True,
        progress_callback: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
        on_limit_reached: Callable[[int], None] | None = None,
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
        if max_results is not None and max_results <= 0:
            raise ValidationError("Max results must be a positive integer")

        include_types = self._normalize_file_types(include_types)
        exclude_types = self._normalize_file_types(exclude_types)
        match_plan = self._build_match_plan(search_term, search_mode, case_sensitive=case_sensitive)
        modified_after_ts = modified_after.timestamp() if modified_after is not None else None
        modified_before_ts = modified_before.timestamp() if modified_before is not None else None

        backend = self._resolve_search_backend(search_within_files, search_mode, search_backend)
        if backend == SearchBackend.RIPGREP:
            try:
                yield from self._ripgrep.search(
                    directory=directory,
                    match_plan=match_plan,
                    include_types=include_types,
                    exclude_types=exclude_types,
                    max_depth=max_depth,
                    min_size=min_size,
                    max_size=max_size,
                    max_results=max_results,
                    modified_after_ts=modified_after_ts,
                    modified_before_ts=modified_before_ts,
                    follow_symlinks=follow_symlinks,
                    include_ignored=include_ignored,
                    context_lines=context_lines,
                    exclude_shots=exclude_shots,
                    progress_callback=progress_callback,
                    cancel_event=cancel_event,
                    on_limit_reached=on_limit_reached,
                )
                return
            except RipgrepUnavailableError:
                self.logger.info("Falling back to Python search backend")

        files_processed = 0
        emitted_results = 0
        inventory = self._inventory.get_snapshot(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            progress_callback=progress_callback,
            cancel_event=cancel_event,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )
        if inventory is None:
            return

        deferred_results: list[SearchResult] | None = (
            [] if not search_within_files and search_mode == SearchMode.FUZZY else None
        )

        self.logger.debug(f"Starting search in directory: {directory}")

        try:
            if match_folders and not search_within_files:
                for dir_path in inventory.directories:
                    if cancel_event and cancel_event.is_set():
                        self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                        return
                    folder_name = os.path.basename(dir_path)
                    result_tuple = self._score_match(
                        folder_name,
                        match_plan,
                        allow_partial_fuzzy=False,
                    )
                    if result_tuple is None:
                        continue
                    score, _, _ = result_tuple
                    result = SearchResult(dir_path, match_score=score)
                    if deferred_results is not None:
                        deferred_results.append(result)
                    else:
                        yield result
                        emitted_results += 1
                        if max_results is not None and emitted_results >= max_results:
                            if on_limit_reached:
                                on_limit_reached(max_results)
                            return

            if search_within_files:
                # Collect filtered entries first
                content_entries: list[InventoryEntry] = []
                for entry in inventory.files:
                    if cancel_event and cancel_event.is_set():
                        self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                        return
                    if not self._check_cached_inventory_filters(
                        entry,
                        include_types=include_types,
                        exclude_types=exclude_types,
                        search_within_files=search_within_files,
                        min_size=min_size,
                        max_size=max_size,
                        modified_after_ts=modified_after_ts,
                        modified_before_ts=modified_before_ts,
                    ):
                        files_processed += 1
                        self._update_cached_inventory_progress(
                            files_processed,
                            len(inventory.files),
                            progress_callback,
                        )
                        continue
                    content_entries.append(entry)
                    files_processed += 1
                    self._update_cached_inventory_progress(
                        files_processed,
                        len(inventory.files),
                        progress_callback,
                    )

                # Process in parallel chunks
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.max_workers
                ) as executor:
                    for chunk_start in range(
                        0, len(content_entries), CONTENT_SEARCH_POOL_CHUNK_SIZE
                    ):
                        if cancel_event and cancel_event.is_set():
                            return
                        chunk = content_entries[
                            chunk_start : chunk_start + CONTENT_SEARCH_POOL_CHUNK_SIZE
                        ]

                        def _make_task(
                            fp: Path,
                            fs: int,
                            mp: MatchPlan = match_plan,
                            cl: int = context_lines,
                        ) -> list[SearchResult]:
                            return list(
                                self._search_file_content(fp, mp, file_size=fs, context_lines=cl)
                            )

                        future_to_entry = {
                            executor.submit(_make_task, Path(e.file_path), e.file_size): e
                            for e in chunk
                        }
                        for future in concurrent.futures.as_completed(future_to_entry):
                            if cancel_event and cancel_event.is_set():
                                return
                            entry = future_to_entry[future]
                            try:
                                results = future.result()
                            except FileAccessError as exc:
                                self.logger.warning(f"Skipping file due to access error: {exc}")
                                continue
                            for result in results:
                                result.mod_time = entry.mod_time
                                yield result
                                emitted_results += 1
                                if max_results is not None and emitted_results >= max_results:
                                    if on_limit_reached:
                                        on_limit_reached(max_results)
                                    return
            else:
                for entry in inventory.files:
                    if cancel_event and cancel_event.is_set():
                        self.logger.info(f"Search cancelled. Files processed: {files_processed}")
                        return

                    if not self._check_cached_inventory_filters(
                        entry,
                        include_types=include_types,
                        exclude_types=exclude_types,
                        search_within_files=search_within_files,
                        min_size=min_size,
                        max_size=max_size,
                        modified_after_ts=modified_after_ts,
                        modified_before_ts=modified_before_ts,
                    ):
                        files_processed += 1
                        self._update_cached_inventory_progress(
                            files_processed,
                            len(inventory.files),
                            progress_callback,
                        )
                        continue

                    try:
                        result_tuple = self._score_match(
                            entry.file_name,
                            match_plan,
                            allow_partial_fuzzy=False,
                        )
                        if result_tuple is not None:
                            score, _, _ = result_tuple
                            result = SearchResult(
                                entry.file_path,
                                mod_time=entry.mod_time,
                                file_size=entry.file_size,
                                match_score=score,
                            )
                            if deferred_results is not None:
                                deferred_results.append(result)
                            else:
                                yield result
                                emitted_results += 1
                                if max_results is not None and emitted_results >= max_results:
                                    if on_limit_reached:
                                        on_limit_reached(max_results)
                                    return
                    except FileAccessError as e:
                        self.logger.warning(f"Skipping file due to access error: {e}")

                    files_processed += 1
                    self._update_cached_inventory_progress(
                        files_processed,
                        len(inventory.files),
                        progress_callback,
                    )

        except PermissionError as e:
            raise DirectoryError(f"Permission denied accessing directory: {directory}") from e
        except FileNotFoundError as e:
            raise DirectoryError(f"Directory not found: {directory}") from e
        except Exception as e:
            self.logger.exception("Unexpected error during search")
            raise SearchError(f"Search operation failed: {e!s}") from e

        if deferred_results:
            deferred_results.sort(
                key=lambda result: (result.match_score or 0.0, result.file_path.lower()),
                reverse=True,
            )
            if max_results is not None and len(deferred_results) > max_results:
                for result in deferred_results[:max_results]:
                    yield result
                if on_limit_reached:
                    on_limit_reached(max_results)
                return
            yield from deferred_results

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

    def _normalize_file_types(self, file_types: list[str] | None) -> list[str]:
        """Normalize file type filters to lower-case extensions."""
        normalized: list[str] = []
        for file_type in file_types or []:
            value = file_type.strip().lower()
            if not value:
                continue
            if not value.startswith(".") and not any(char in value for char in "*?[]"):
                value = f".{value}"
            normalized.append(value)
        return normalized

    def clear_inventory_cache(
        self,
        directory: str,
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool,
        exclude_shots: bool = True,
    ) -> None:
        """Public API: evict a specific inventory from both caches."""
        self._inventory.clear_cache(
            directory=directory,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
            exclude_shots=exclude_shots,
        )

    def _check_cached_inventory_filters(
        self,
        entry: InventoryEntry,
        *,
        include_types: list[str],
        exclude_types: list[str],
        search_within_files: bool,
        min_size: int | None,
        max_size: int | None,
        modified_after_ts: float | None,
        modified_before_ts: float | None,
    ) -> bool:
        """Apply query filters against cached inventory metadata."""
        if not check_file_filters(
            entry.file_size,
            entry.mod_time,
            min_size=min_size,
            max_size=max_size,
            modified_after_ts=modified_after_ts,
            modified_before_ts=modified_before_ts,
        ):
            return False
        return self._should_process_file(
            entry.file_lower,
            include_types,
            exclude_types,
            search_within_files=search_within_files,
            file_path=Path(entry.file_path),
        )

    def _build_match_plan(
        self, search_term: str, search_mode: SearchMode, case_sensitive: bool = False
    ) -> MatchPlan:
        """Compile reusable match state for the active query."""
        normalized_term = search_term if case_sensitive else search_term.lower()
        regex: re.Pattern[str] | None = None
        if search_mode == SearchMode.REGEX:
            try:
                flags = 0 if case_sensitive else re.IGNORECASE
                regex = re.compile(search_term, flags)
            except re.error as exc:
                raise ValidationError(f"Invalid regular expression: {exc}") from exc
        return MatchPlan(
            mode=search_mode,
            raw_term=search_term,
            normalized_term=normalized_term,
            regex=regex,
            case_sensitive=case_sensitive,
        )

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
        file_ext = Path(file_lower).suffix.lower()
        if search_within_files and file_ext in self._always_binary_extensions:
            return False
        # Maybe-binary formats (e.g. .usd) can be text or binary — sniff before skipping.
        if (
            search_within_files
            and file_ext in self._maybe_binary_extensions
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

    def _score_match(
        self,
        text: str,
        match_plan: MatchPlan,
        *,
        allow_partial_fuzzy: bool,
    ) -> tuple[float, int, int] | None:
        """Return (score, match_start, match_length), or None when no match."""
        if match_plan.mode == SearchMode.SUBSTRING:
            haystack = text if match_plan.case_sensitive else text.lower()
            idx = haystack.find(match_plan.normalized_term)
            if idx >= 0:
                return (100.0, idx, len(match_plan.normalized_term))
            return None
        if match_plan.mode == SearchMode.GLOB:
            pattern = ensure_glob_wildcard(match_plan.raw_term)
            if match_plan.case_sensitive:
                matched = fnmatch.fnmatch(text, pattern)
            else:
                matched = fnmatch.fnmatch(text.lower(), pattern.lower())
            if matched:
                return (100.0, 0, len(text))
            return None
        if match_plan.mode == SearchMode.REGEX:
            assert match_plan.regex is not None
            m = match_plan.regex.search(text)
            if m:
                return (100.0, m.start(), m.end() - m.start())
            return None
        if match_plan.mode == SearchMode.FUZZY:
            score = self._score_fuzzy_match(
                text, match_plan.normalized_term, allow_partial_fuzzy=allow_partial_fuzzy
            )
            if score is not None:
                return (score, 0, 0)  # No precise position for fuzzy
            return None
        raise AssertionError(f"Unhandled SearchMode: {match_plan.mode!r}")

    def _score_fuzzy_match(
        self, text: str, normalized_term: str, *, allow_partial_fuzzy: bool
    ) -> float | None:
        """Compute a fuzzy score with tighter thresholds for filename matching."""
        if not RAPIDFUZZ_AVAILABLE or not normalized_term:
            return None
        assert fuzz is not None

        normalized_text = text.lower()
        if normalized_text == normalized_term:
            return 120.0

        wratio = float(fuzz.WRatio(normalized_term, normalized_text))  # pyright: ignore[reportPossiblyUnboundVariable]
        score = wratio
        if allow_partial_fuzzy:
            partial = float(
                fuzz.partial_ratio(normalized_term, normalized_text)  # pyright: ignore[reportPossiblyUnboundVariable]
            )
            score = max(score, partial * 0.9)
            threshold = FUZZY_PARTIAL_THRESHOLD
        else:
            simple = float(fuzz.ratio(normalized_term, normalized_text))  # pyright: ignore[reportPossiblyUnboundVariable]
            partial = float(
                fuzz.partial_ratio(normalized_term, normalized_text)  # pyright: ignore[reportPossiblyUnboundVariable]
            )
            score = max(score, simple, partial * 0.85)
            threshold = FUZZY_FULL_THRESHOLD
            if normalized_text.startswith(normalized_term):
                score += FUZZY_EXACT_BONUS
            elif normalized_term in normalized_text:
                score += FUZZY_WORD_BONUS

        return score if score >= threshold else None

    def _detect_bom(self, file_path: Path) -> str | None:
        """Read up to 4 bytes and return the encoding if a BOM is present, else None."""
        try:
            with open(file_path, "rb") as f:
                raw = f.read(4)
            if raw[:3] == codecs.BOM_UTF8:
                return "utf-8-sig"
            if raw[:4] in (codecs.BOM_UTF32_LE, codecs.BOM_UTF32_BE):
                return "utf-32"
            if raw[:2] in (codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE):
                return "utf-16"
        except OSError:
            return None
        return None

    def _search_file_content(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int = 0,
        context_lines: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search within file content for the search term using optimized methods."""
        if file_size == 0:
            return
        if file_size > LARGE_FILE_MMAP_THRESHOLD:
            yield from self._search_large_file(
                file_path, match_plan, file_size=file_size, context_lines=context_lines
            )
        else:
            yield from self._search_small_file(
                file_path, match_plan, file_size=file_size, context_lines=context_lines
            )

    def _search_small_file(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int | None = None,
        context_lines: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search small files using standard file reading."""
        try:
            encoding = self._detect_bom(file_path) or "utf-8"
            with open(file_path, encoding=encoding, errors="replace") as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                result_tuple = self._score_match(line, match_plan, allow_partial_fuzzy=True)
                if result_tuple is not None:
                    score, raw_match_start, match_length = result_tuple
                    line_content = truncate_line(line.strip())
                    next_line = lines[i + 1].strip() if i + 1 < len(lines) else None
                    if next_line:
                        next_line = truncate_line(next_line)

                    # Adjust match_start for stripped leading whitespace
                    strip_offset = len(line) - len(line.lstrip())
                    match_start = max(0, raw_match_start - strip_offset)

                    ctx_before, ctx_after, ctx_next = _collect_context(lines, i, context_lines)
                    if ctx_next is not None:
                        next_line = ctx_next

                    yield SearchResult(
                        str(file_path),
                        i + 1,
                        line_content,
                        next_line,
                        file_size=file_size,
                        match_score=score,
                        context_before=ctx_before,
                        context_after=ctx_after,
                        match_start=match_start,
                        match_length=match_length,
                    )
        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except OSError as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _search_large_file(
        self,
        file_path: Path,
        match_plan: MatchPlan,
        file_size: int | None = None,
        context_lines: int = 0,
    ) -> Generator[SearchResult, None, None]:
        """Search large files using memory mapping for better performance."""
        try:
            with open(file_path, "rb") as f:
                try:
                    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                        encoding = self._detect_bom(file_path) or "utf-8"
                        line_no = 0
                        pos = 0
                        maxlen = context_lines if context_lines > 0 else 0
                        prev_lines: deque[str] = deque(maxlen=maxlen)
                        while pos < len(mm):
                            eol = mm.find(b"\n", pos)
                            end = eol if eol != -1 else len(mm)
                            line_bytes = mm[pos:end]
                            line_text = line_bytes.decode(encoding, errors="replace").rstrip("\r")
                            line_no += 1
                            result_tuple = self._score_match(
                                line_text,
                                match_plan,
                                allow_partial_fuzzy=True,
                            )
                            if result_tuple is not None:
                                score, raw_match_start, match_length = result_tuple
                                line_content = truncate_line(line_text.strip())
                                # Adjust match_start for stripped leading whitespace
                                strip_offset = len(line_text) - len(line_text.lstrip())
                                match_start = max(0, raw_match_start - strip_offset)
                                # Scan forward for next_line and context_after
                                after_lines: list[str] = []
                                scan_count = max(1, context_lines)
                                scan_pos = end + 1
                                if eol != -1:
                                    for _ in range(scan_count):
                                        if scan_pos >= len(mm):
                                            break
                                        next_eol = mm.find(b"\n", scan_pos)
                                        next_end = next_eol if next_eol != -1 else len(mm)
                                        raw_next = (
                                            mm[scan_pos:next_end]
                                            .decode(encoding, errors="replace")
                                            .rstrip("\r")
                                            .strip()
                                        )
                                        if raw_next:
                                            after_lines.append(truncate_line(raw_next))
                                        scan_pos = next_end + 1
                                next_line = after_lines[0] if after_lines else None
                                ctx_before: list[str] | None = None
                                ctx_after: list[str] | None = None
                                if context_lines > 0:
                                    ctx_before = list(prev_lines)
                                    ctx_after = after_lines if after_lines else None
                                yield SearchResult(
                                    str(file_path),
                                    line_no,
                                    line_content,
                                    next_line,
                                    file_size=file_size,
                                    match_score=score,
                                    context_before=ctx_before,
                                    context_after=ctx_after,
                                    match_start=match_start,
                                    match_length=match_length,
                                )
                            prev_lines.append(line_text.strip())
                            pos = end + 1

                except (OSError, ValueError):
                    # Fallback to regular file reading if mmap fails
                    yield from self._search_small_file(
                        file_path, match_plan, file_size=file_size, context_lines=context_lines
                    )

        except PermissionError as e:
            raise FileAccessError(f"Permission denied reading file: {file_path}") from e
        except UnicodeDecodeError:
            self.logger.debug(f"Skipping binary file: {file_path}")
        except OSError as e:
            raise FileAccessError(f"Error reading file {file_path}: {e}") from e

    def _update_cached_inventory_progress(
        self,
        files_processed: int,
        total_files: int,
        progress_callback: Callable[[str], None] | None,
    ) -> None:
        """Update progress while iterating an already-built inventory."""
        if progress_callback and files_processed % 100 == 0:
            progress_callback(f"Searching cached inventory: {files_processed}/{total_files} files")
