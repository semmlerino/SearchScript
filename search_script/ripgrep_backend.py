import json
import logging
import os
import re
import subprocess
import threading
from base64 import b64decode
from collections.abc import Callable, Generator
from pathlib import Path
from queue import Empty, Queue

from .config import SearchError
from .constants import RIPGREP_PROGRESS_MILESTONE
from .models import (
    MatchPlan,
    SearchMode,
    SearchResult,
    check_file_filters,
    ensure_glob_wildcard,
    truncate_line,
)


class RipgrepUnavailableError(Exception):
    pass


class RipgrepBackend:
    def __init__(self, logger: logging.Logger, rg_path: str | None, max_workers: int = 4):
        self.logger = logger
        self._rg_path = rg_path
        self.max_workers = max_workers

    def search(
        self,
        directory: str,
        match_plan: MatchPlan,
        include_types: list[str],
        exclude_types: list[str],
        max_depth: int | None,
        min_size: int | None,
        max_size: int | None,
        max_results: int | None,
        modified_after_ts: float | None,
        modified_before_ts: float | None,
        follow_symlinks: bool,
        include_ignored: bool = True,
        context_lines: int = 0,
        exclude_shots: bool = True,
        progress_callback: Callable[[str], None] | None = None,
        cancel_event: threading.Event | None = None,
        on_limit_reached: Callable[[int], None] | None = None,
    ) -> Generator[SearchResult, None, None]:
        """Search file contents with ripgrep and filter matches using the app's metadata rules."""
        if self._rg_path is None:
            raise RipgrepUnavailableError("ripgrep not found")

        command = self._build_command(
            directory=directory,
            match_plan=match_plan,
            include_types=include_types,
            exclude_types=exclude_types,
            max_depth=max_depth,
            follow_symlinks=follow_symlinks,
            include_ignored=include_ignored,
            context_lines=context_lines,
            exclude_shots=exclude_shots,
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
            self.logger.warning(f"ripgrep backend unavailable: {e}")
            raise RipgrepUnavailableError(str(e)) from e

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
        emitted_results = 0
        pending_context: list[str] = []

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
                    self.logger.debug("Skipping non-JSON ripgrep output line: %r", raw_line)
                    continue

                msg_type = payload.get("type")
                if msg_type == "context":
                    ctx_data = payload.get("data", {})
                    ctx_text = self._decode_text(ctx_data.get("lines", {})).strip()
                    ctx_text = truncate_line(ctx_text)
                    pending_context.append(ctx_text)
                    continue
                if msg_type != "match":
                    pending_context.clear()
                    continue

                data = payload["data"]
                file_path = self._resolve_path(search_root, data["path"])
                cache_key = str(file_path)
                stat_result = stat_cache.get(cache_key)
                if stat_result is None:
                    try:
                        stat_result = file_path.stat(follow_symlinks=follow_symlinks)
                    except OSError:
                        continue
                    stat_cache[cache_key] = stat_result

                if not check_file_filters(
                    stat_result.st_size,
                    stat_result.st_mtime,
                    min_size=min_size,
                    max_size=max_size,
                    modified_after_ts=modified_after_ts,
                    modified_before_ts=modified_before_ts,
                ):
                    continue

                raw_line_text = self._decode_text(data["lines"]).rstrip("\n").rstrip("\r")
                stripped_line = raw_line_text.strip()
                stripped_line = truncate_line(stripped_line)

                # Parse submatch positions for highlighting
                submatches = data.get("submatches", [])
                rg_match_start: int | None = None
                rg_match_length: int | None = None
                if submatches:
                    sm = submatches[0]
                    sm_start = sm.get("start")
                    sm_end = sm.get("end")
                    if sm_start is not None and sm_end is not None:
                        strip_offset = len(raw_line_text) - len(raw_line_text.lstrip())
                        rg_match_start = max(0, sm_start - strip_offset)
                        rg_match_length = sm_end - sm_start

                # Collect context lines from ripgrep output
                rg_ctx_before: list[str] | None = None
                rg_ctx_after: list[str] | None = None
                if context_lines > 0:
                    rg_ctx_before = list(pending_context) if pending_context else None
                    pending_context.clear()
                    # Lookahead: read context entries after the match
                    after_lines: list[str] = []
                    putback: list[str] = []
                    while len(after_lines) < context_lines:
                        try:
                            peek_line = stdout_queue.get(timeout=0.1)
                        except Empty:
                            if process.poll() is not None:
                                break
                            continue
                        if peek_line is None:
                            stdout_queue.put(None)
                            break
                        try:
                            peek_payload = json.loads(peek_line)
                        except json.JSONDecodeError:
                            continue
                        if peek_payload.get("type") == "context":
                            peek_data = peek_payload.get("data", {})
                            peek_text = self._decode_text(peek_data.get("lines", {})).strip()
                            peek_text = truncate_line(peek_text)
                            after_lines.append(peek_text)
                        else:
                            putback.append(peek_line)
                            break
                    for pb in putback:
                        stdout_queue.put(pb)
                    rg_ctx_after = after_lines if after_lines else None
                else:
                    pending_context.clear()

                matches += 1
                if progress_callback and matches % RIPGREP_PROGRESS_MILESTONE == 0:
                    progress_callback(f"ripgrep matched {matches} lines in {directory}")

                yield SearchResult(
                    str(file_path),
                    data.get("line_number"),
                    stripped_line,
                    file_size=stat_result.st_size,
                    mod_time=stat_result.st_mtime,
                    context_before=rg_ctx_before,
                    context_after=rg_ctx_after,
                    match_start=rg_match_start,
                    match_length=rg_match_length,
                )
                emitted_results += 1
                if max_results is not None and emitted_results >= max_results:
                    if on_limit_reached:
                        on_limit_reached(max_results)
                    return

            return_code = process.wait()
            if return_code not in (0, 1):
                stderr = ""
                if process.stderr is not None:
                    stderr = process.stderr.read().strip()
                raise SearchError(stderr or f"ripgrep search failed with exit code {return_code}")
        finally:
            self._terminate_process(process)

    def _build_command(
        self,
        directory: str,
        match_plan: MatchPlan,
        include_types: list[str],
        exclude_types: list[str],
        max_depth: int | None,
        follow_symlinks: bool,
        include_ignored: bool = True,
        context_lines: int = 0,
        exclude_shots: bool = True,
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
            "--threads",
            str(self.max_workers),
        ]
        if include_ignored:
            command.append("-uu")
        if follow_symlinks:
            command.append("-L")
        if max_depth is not None:
            command.extend(["--max-depth", str(max_depth)])
        if context_lines > 0:
            command.extend(["-C", str(context_lines)])
        for ext in include_types:
            command.extend(["-g", f"*{ext}"])
        for ext in exclude_types:
            command.extend(["-g", f"!*{ext}"])
        if exclude_shots:
            command.extend(["-g", "!shots/"])

        pattern = match_plan.raw_term
        if match_plan.mode == SearchMode.SUBSTRING:
            command.append("--fixed-strings")
        elif match_plan.mode == SearchMode.GLOB:
            pattern = self._translate_glob_to_regex(match_plan.raw_term)

        if not match_plan.case_sensitive:
            command.append("-i")
            command.append("--glob-case-insensitive")
        command.extend([pattern, directory])
        return command

    def _translate_glob_to_regex(self, search_term: str) -> str:
        """Translate the app's line-glob semantics into a ripgrep-compatible regex."""
        pattern = ensure_glob_wildcard(search_term)
        translated: list[str] = []
        idx = 0
        while idx < len(pattern):
            char = pattern[idx]
            if char == "*":
                translated.append(".*")
            elif char == "?":
                translated.append(".")
            elif char == "[":
                end = idx + 1
                while end < len(pattern) and pattern[end] != "]":
                    end += 1
                if end >= len(pattern):
                    translated.append(r"\[")
                else:
                    contents = pattern[idx + 1 : end]
                    negate = contents.startswith(("!", "^"))
                    if negate:
                        contents = contents[1:]
                    safe_contents: list[str] = []
                    for pos, content_char in enumerate(contents):
                        if content_char == "\\":
                            safe_contents.append(r"\\")
                        elif content_char == "]":
                            safe_contents.append(r"\]")
                        elif content_char == "^" and pos == 0:
                            safe_contents.append(r"\^")
                        else:
                            safe_contents.append(content_char)
                    prefix = "^" if negate else ""
                    translated.append(f"[{prefix}{''.join(safe_contents)}]")
                    idx = end
            else:
                translated.append(re.escape(char))
            idx += 1
        return f"^{''.join(translated)}$"

    def _resolve_path(self, search_root: Path, path_info: dict[str, str]) -> Path:
        """Resolve ripgrep's match path into an absolute path under the searched root."""
        raw_path = self._decode_text(path_info)
        path = Path(raw_path)
        if path.is_absolute():
            return path
        return search_root / path

    def _decode_text(self, payload: dict[str, str]) -> str:
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
