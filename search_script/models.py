from __future__ import annotations

import importlib.util as _importlib_util
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .constants import LINE_CONTENT_MAX_CHARS

RAPIDFUZZ_AVAILABLE: bool = _importlib_util.find_spec("rapidfuzz") is not None


@dataclass
class SearchResult:
    file_path: str
    line_number: int | None = None
    line_content: str | None = None
    next_line: str | None = None
    mod_time: float | None = None
    file_size: int | None = None
    match_score: float | None = None
    context_before: list[str] | None = None
    context_after: list[str] | None = None
    match_start: int | None = None
    match_length: int | None = None

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


@dataclass(frozen=True)
class MatchPlan:
    mode: SearchMode
    raw_term: str
    normalized_term: str
    regex: re.Pattern[str] | None = None
    case_sensitive: bool = False


@dataclass(frozen=True)
class SearchParams:
    directory: str
    search_term: str
    include_types: list[str] | None = None
    exclude_types: list[str] | None = None
    search_within_files: bool = False
    search_mode: SearchMode = SearchMode.SUBSTRING
    search_backend: SearchBackend = SearchBackend.AUTO
    max_depth: int | None = None
    min_size: int | None = None
    max_size: int | None = None
    max_results: int | None = None
    modified_after: datetime | None = None
    modified_before: datetime | None = None
    match_folders: bool = False
    follow_symlinks: bool = False
    include_ignored: bool = True
    context_lines: int = 0
    case_sensitive: bool = False
    exclude_shots: bool = True


@dataclass
class ResultBatchMsg:
    results: list[SearchResult]


@dataclass
class DoneMsg:
    count: int


@dataclass
class ErrorMsg:
    message: str


@dataclass
class CancelledMsg:
    message: str


@dataclass
class StatusMsg:
    message: str


@dataclass
class LimitReachedMsg:
    limit: int


SearchMessage = ResultBatchMsg | DoneMsg | ErrorMsg | CancelledMsg | StatusMsg | LimitReachedMsg


def check_file_filters(
    file_size: int,
    mod_time: float,
    *,
    min_size: int | None,
    max_size: int | None,
    modified_after_ts: float | None,
    modified_before_ts: float | None,
) -> bool:
    """Return True if metadata passes all size/date filters."""
    if min_size is not None and file_size < min_size:
        return False
    if max_size is not None and file_size > max_size:
        return False
    if modified_after_ts is not None and mod_time < modified_after_ts:
        return False
    return modified_before_ts is None or mod_time <= modified_before_ts


def truncate_line(text: str) -> str:
    if len(text) > LINE_CONTENT_MAX_CHARS:
        return text[:LINE_CONTENT_MAX_CHARS] + "..."
    return text


def ensure_glob_wildcard(term: str) -> str:
    """Wrap term in wildcards if it contains no explicit glob characters."""
    return term if any(c in term for c in "*?[]") else f"*{term}*"
