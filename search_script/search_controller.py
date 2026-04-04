import queue
import threading
from datetime import datetime
from time import monotonic
from typing import Any

from PySide6.QtCore import QTimer

from .constants import (
    PROCESS_RESULTS_TIME_BUDGET_S,
    RESULT_BATCH_SIZE,
    RESULT_FIRST_BATCH_SIZE,
    RESULT_POLL_BACKOFF_DELAY_MS,
    RESULT_POLL_INITIAL_DELAY_MS,
)
from .file_utils import FileOperations, LoggingConfig
from .models import (
    CancelledMsg,
    DoneMsg,
    ErrorMsg,
    LimitReachedMsg,
    ResultBatchMsg,
    SearchBackend,
    SearchMessage,
    SearchMode,
    SearchParams,
    SearchResult,
    StatusMsg,
)
from .search_engine import SearchEngine
from .ui_components import SearchUI


class SearchController:
    """Main controller coordinating UI and search operations."""

    def __init__(self):
        self.logger = LoggingConfig.setup_logging()

        # Components
        self.search_engine = SearchEngine(self.logger)
        self.ui = SearchUI(self.logger)
        self.file_ops = FileOperations(self.logger)

        # Threading
        self.cancel_event = threading.Event()
        self.search_thread: threading.Thread | None = None
        self.result_queue: queue.Queue[SearchMessage] = queue.Queue()
        self.search_was_truncated = False
        self.search_result_limit: int | None = None
        self._last_search_params: SearchParams | None = None
        self._search_generation: int = 0

        # Setup callbacks
        self._setup_callbacks()

        self.logger.info("SearchController initialized")

    def _setup_callbacks(self):
        """Connect UI signals to controller slots."""
        self.ui.search_requested.connect(self._start_search)
        self.ui.search_cancelled.connect(self._cancel_search)
        self.ui.refresh_requested.connect(self._refresh_search)
        self.ui.result_double_clicked.connect(self._open_file)
        self.ui.open_folder_requested.connect(self._open_containing_folder)
        self.ui.clear_dates_btn.clicked.connect(self._clear_dates)

    def _clear_dates(self) -> None:
        """Reset date filter widgets to their minimum (no-filter) state."""
        self.ui.modified_after_entry.setDate(self.ui.modified_after_entry.minimumDate())
        self.ui.modified_before_entry.setDate(self.ui.modified_before_entry.minimumDate())

    def _build_modified_date_filters(self) -> tuple[datetime | None, datetime | None]:
        """Convert the date widgets into inclusive datetime bounds."""
        min_date = self.ui.modified_after_entry.minimumDate()
        after_qdate = self.ui.modified_after_entry.date()
        modified_after = (
            datetime(after_qdate.year(), after_qdate.month(), after_qdate.day())
            if after_qdate != min_date
            else None
        )

        before_qdate = self.ui.modified_before_entry.date()
        modified_before = (
            datetime(
                before_qdate.year(),
                before_qdate.month(),
                before_qdate.day(),
                23,
                59,
                59,
                999999,
            )
            if before_qdate != min_date
            else None
        )
        return modified_after, modified_before

    def _start_search(self, search_params: dict[str, Any]):
        """Start the search operation."""
        self.logger.info(f"Starting search with parameters: {search_params}")
        modified_after, modified_before = self._build_modified_date_filters()
        params = SearchParams(
            directory=search_params["directory"],
            search_term=search_params["search_term"],
            include_types=search_params["include_types"],
            exclude_types=search_params["exclude_types"],
            search_within_files=search_params["search_within_files"],
            search_mode=SearchMode(search_params.get("search_mode", "substring")),
            search_backend=SearchBackend(search_params.get("search_backend", "auto")),
            max_depth=search_params.get("max_depth"),
            min_size=search_params.get("min_size"),
            max_size=search_params.get("max_size"),
            max_results=search_params.get("max_results"),
            modified_after=modified_after,
            modified_before=modified_before,
            match_folders=search_params.get("match_folders", False),
            follow_symlinks=search_params.get("follow_symlinks", False),
            include_ignored=search_params.get("include_ignored", True),
            context_lines=search_params.get("context_lines", 0),
            case_sensitive=search_params.get("case_sensitive", False),
            exclude_shots=search_params.get("exclude_shots", True),
        )
        self._start_search_from_params(params)

    def _start_search_from_params(self, params: SearchParams) -> None:
        """Start search from already-constructed SearchParams."""
        self._last_search_params = params

        self._search_generation += 1
        generation = self._search_generation
        if self.search_thread is not None and self.search_thread.is_alive():
            self.cancel_event.set()

        self.ui.set_search_state(True)
        self.ui.clear_results()
        self.ui.update_status("Starting search...")

        self.cancel_event = threading.Event()
        cancel_event = self.cancel_event
        self.result_queue = queue.Queue()
        self.search_was_truncated = False
        self.search_result_limit = params.max_results

        self.search_thread = threading.Thread(
            target=self._search_worker, args=(params, cancel_event), daemon=True
        )
        self.search_thread.start()

        QTimer.singleShot(
            RESULT_POLL_INITIAL_DELAY_MS,
            lambda g=generation: self._process_results(g),  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType]
        )

    def _search_worker(self, params: SearchParams, cancel_event: threading.Event):
        """Worker thread for search operations."""
        try:
            count = 0
            batch: list[SearchResult] = []
            first_flushed = False

            def progress_callback(message: str):
                self.result_queue.put(StatusMsg(message))

            def on_limit_reached(limit: int) -> None:
                self.result_queue.put(LimitReachedMsg(limit))

            def flush_batch() -> None:
                nonlocal batch
                if batch:
                    self.result_queue.put(ResultBatchMsg(batch))
                    batch = []

            for result in self.search_engine.search_files(
                directory=params.directory,
                search_term=params.search_term,
                include_types=params.include_types,
                exclude_types=params.exclude_types,
                search_within_files=params.search_within_files,
                search_mode=params.search_mode,
                search_backend=params.search_backend,
                max_depth=params.max_depth,
                min_size=params.min_size,
                max_size=params.max_size,
                max_results=params.max_results,
                modified_after=params.modified_after,
                modified_before=params.modified_before,
                match_folders=params.match_folders,
                follow_symlinks=params.follow_symlinks,
                include_ignored=params.include_ignored,
                context_lines=params.context_lines,
                case_sensitive=params.case_sensitive,
                exclude_shots=params.exclude_shots,
                progress_callback=progress_callback,
                on_limit_reached=on_limit_reached,
                cancel_event=cancel_event,
            ):
                if cancel_event.is_set():
                    flush_batch()
                    msg = f"Search cancelled. Found {count} matches."
                    self.result_queue.put(CancelledMsg(msg))
                    return
                batch.append(result)
                count += 1
                threshold = RESULT_FIRST_BATCH_SIZE if not first_flushed else RESULT_BATCH_SIZE
                if len(batch) >= threshold:
                    flush_batch()
                    first_flushed = True

            flush_batch()
            self.result_queue.put(DoneMsg(count))

        except Exception as e:
            error_msg = f"Search error: {e!s}"
            self.result_queue.put(ErrorMsg(error_msg))
            self.logger.error(error_msg)

    def _process_results(self, generation: int):
        """Process results from the search thread."""
        if generation != self._search_generation:
            return  # Stale generation — discard

        batch: list[SearchResult] = []
        started_at = monotonic()
        try:
            while monotonic() - started_at < PROCESS_RESULTS_TIME_BUDGET_S:
                msg = self.result_queue.get_nowait()

                if isinstance(msg, ResultBatchMsg):
                    batch.extend(msg.results)
                elif isinstance(msg, DoneMsg):
                    if batch:
                        self.ui.add_results_batch(batch)
                    self._drain_remaining_results()
                    self._handle_search_complete()
                    return
                elif isinstance(msg, StatusMsg):
                    self.ui.update_status(msg.message)
                elif isinstance(msg, LimitReachedMsg):
                    self.search_was_truncated = True
                    self.search_result_limit = msg.limit
                elif isinstance(msg, ErrorMsg):
                    if batch:
                        self.ui.add_results_batch(batch)
                    self._handle_search_error(msg.message)
                    return
                else:  # CancelledMsg
                    if batch:
                        self.ui.add_results_batch(batch)
                    self._drain_remaining_results()
                    self._handle_search_cancelled(msg.message)
                    return
        except queue.Empty:
            pass

        if batch:
            self.ui.add_results_batch(batch)

        if self.search_thread and self.search_thread.is_alive():
            delay_ms = 0 if not self.result_queue.empty() else RESULT_POLL_BACKOFF_DELAY_MS
            QTimer.singleShot(delay_ms, lambda g=generation: self._process_results(g))  # pyright: ignore[reportUnknownLambdaType,reportUnknownArgumentType]
        else:
            sentinel = self._drain_remaining_results()
            if sentinel is not None:
                if isinstance(sentinel, ErrorMsg):
                    self._handle_search_error(sentinel.message)
                elif isinstance(sentinel, CancelledMsg):
                    self._handle_search_cancelled(sentinel.message)
                else:
                    self._handle_search_complete()
            else:
                self._handle_search_complete()

    def _handle_search_complete(self):
        """Handle search completion."""
        self.ui.set_search_state(False)
        displayed, file_count = self.ui.get_result_summary()
        if self.search_was_truncated and self.search_result_limit is not None:
            self.ui.update_status(
                f"Search completed. Showing {displayed} matches across {file_count} files "
                f"(limit {self.search_result_limit})."
            )
        else:
            self.ui.update_status(
                f"Search completed. {displayed} matches across {file_count} files."
            )

        if displayed > 0:
            self.logger.info(f"Search completed with {displayed} matches")
        else:
            self.ui.update_status("Search completed. No matches found.")
            self.logger.info("Search completed with no matches")

    def _handle_search_error(self, error_msg: str):
        """Handle search error."""
        self.ui.set_search_state(False)
        self.ui.update_status("Search failed.")
        self.ui.show_error_message("Search Error", error_msg)
        self.logger.error(error_msg)

    def _handle_search_cancelled(self, message: str):
        """Handle search cancellation."""
        self.ui.set_search_state(False)
        displayed, file_count = self.ui.get_result_summary()
        status = f"Search cancelled. {displayed} matches across {file_count} files shown."
        if self.search_was_truncated and self.search_result_limit is not None:
            status += f" Result limit {self.search_result_limit} had already been reached."
        self.ui.update_status(status)
        self.logger.info(message)

    def _drain_remaining_results(self) -> SearchMessage | None:
        """Drain remaining results from the queue. Returns first non-result sentinel found."""
        batch: list[SearchResult] = []
        try:
            while True:
                msg = self.result_queue.get_nowait()
                if isinstance(msg, ResultBatchMsg):
                    batch.extend(msg.results)
                elif isinstance(msg, LimitReachedMsg):
                    self.search_was_truncated = True
                    self.search_result_limit = msg.limit
                else:
                    if batch:
                        self.ui.add_results_batch(batch)
                    return msg
        except queue.Empty:
            if batch:
                self.ui.add_results_batch(batch)
            return None

    def _refresh_search(self) -> None:
        """Clear the inventory cache and re-run the last search."""
        if self._last_search_params is None:
            return
        self._cancel_search()
        params = self._last_search_params
        self.search_engine.clear_inventory_cache(
            directory=params.directory,
            max_depth=params.max_depth,
            follow_symlinks=params.follow_symlinks,
            include_ignored=params.include_ignored,
            exclude_shots=params.exclude_shots,
        )
        self._start_search_from_params(params)

    def _cancel_search(self):
        """Cancel the current search operation."""
        self.cancel_event.set()
        self.ui.update_status("Search cancellation requested.")
        self.logger.info("Search cancellation requested")

    def _open_file(self, result: dict[str, Any]):
        """Open a file from the results."""
        file_path = str(result.get("file_path", ""))
        line_number = result.get("line_number")
        if not self.file_ops.open_file(file_path, line_number=line_number):
            self.ui.show_error_message("Error", f"Cannot open file: {file_path}")

    def _open_containing_folder(self, file_path: str):
        """Open the containing folder for a file."""
        if not self.file_ops.open_containing_folder(file_path):
            self.ui.show_error_message("Error", f"Cannot open containing folder for: {file_path}")

    def run(self):
        """Start the application."""
        self.logger.info("Application started")
        self.ui.show()
