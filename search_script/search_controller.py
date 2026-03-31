import queue
import threading
from typing import Any

from PySide6.QtCore import QTimer

from .file_utils import FileOperations, LoggingConfig
from .search_engine import SearchEngine, SearchMode, SearchResult
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
        self.result_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._search_history: list = []

        # Setup callbacks
        self._setup_callbacks()

        self.logger.info("SearchController initialized")

    def _setup_callbacks(self):
        """Setup UI event callbacks."""
        self.ui.on_search_start = self._start_search
        self.ui.on_search_cancel = self._cancel_search
        self.ui.on_result_double_click = self._open_file
        self.ui.on_open_containing_folder = self._open_containing_folder
        self.ui.on_export = self._export_results

    def _start_search(self, search_params: dict[str, Any]):
        """Start the search operation."""
        self.logger.info(f"Starting search with parameters: {search_params}")

        # Track search history
        term = search_params["search_term"]
        if term not in self._search_history:
            self._search_history.insert(0, term)
            self._search_history = self._search_history[:10]
            self.ui.set_search_history(self._search_history)

        # Reset UI state
        self.ui.set_search_state(True)
        self.ui.clear_results()
        self.ui.update_status("Starting search...")

        # Setup threading
        self.cancel_event.clear()
        self.result_queue = queue.Queue()

        # Start search thread
        self.search_thread = threading.Thread(
            target=self._search_worker, args=(search_params,), daemon=True
        )
        self.search_thread.start()

        # Start monitoring results
        QTimer.singleShot(100, self._process_results)

    def _search_worker(self, search_params: dict[str, Any]):
        """Worker thread for search operations."""
        try:
            count = 0

            def progress_callback(message: str):
                self.result_queue.put(("status", message))

            mode_str = search_params.get("search_mode", "substring")
            search_mode = SearchMode(mode_str)

            for result in self.search_engine.search_files(
                directory=search_params["directory"],
                search_term=search_params["search_term"],
                include_types=search_params["include_types"],
                exclude_types=search_params["exclude_types"],
                search_within_files=search_params["search_within_files"],
                search_mode=search_mode,
                max_depth=search_params.get("max_depth"),
                min_size=search_params.get("min_size"),
                max_size=search_params.get("max_size"),
                match_folders=search_params.get("match_folders", False),
                follow_symlinks=search_params.get("follow_symlinks", False),
                progress_callback=progress_callback,
                cancel_event=self.cancel_event,
            ):
                if self.cancel_event.is_set():
                    msg = f"Search cancelled. Found {count} matches."
                    self.result_queue.put(("cancelled", msg))
                    return
                self.result_queue.put(("result", result))
                count += 1

            self.result_queue.put(("done", count))

        except Exception as e:
            error_msg = f"Search error: {e!s}"
            self.result_queue.put(("error", error_msg))
            self.logger.error(error_msg)

    def _process_results(self):
        """Process results from the search thread."""
        try:
            items_this_tick = 0
            while items_this_tick < 200:
                msg_type, data = self.result_queue.get_nowait()

                if msg_type == "result":
                    result: SearchResult = data  # type: ignore[assignment]
                    self.ui.add_result(  # type: ignore[union-attr]
                        result.file_path, result.display_text, result.formatted_mod_time
                    )
                    items_this_tick += 1
                elif msg_type == "done":
                    self._drain_remaining_results()
                    self._handle_search_complete()
                    return
                elif msg_type == "status":
                    self.ui.update_status(str(data))
                elif msg_type == "error":
                    self._handle_search_error(str(data))
                    return
                elif msg_type == "cancelled":
                    self._drain_remaining_results()
                    self._handle_search_cancelled(str(data))
                    return
        except queue.Empty:
            pass

        if self.search_thread and self.search_thread.is_alive():
            QTimer.singleShot(100, self._process_results)
        else:
            sentinel = self._drain_remaining_results()
            if sentinel is not None:
                msg_type, data = sentinel
                if msg_type == "error":
                    self._handle_search_error(str(data))
                elif msg_type == "cancelled":
                    self._handle_search_cancelled(str(data))
                else:
                    self._handle_search_complete()
            else:
                self._handle_search_complete()

    def _handle_search_complete(self):
        """Handle search completion."""
        self.ui.set_search_state(False)
        displayed = self.ui.results_tree.topLevelItemCount()
        self.ui.update_status("Search completed.")

        if displayed > 0:
            self.logger.info(f"Search completed with {displayed} matches")
        else:
            self.ui.show_no_results_message()
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
        displayed = self.ui.results_tree.topLevelItemCount()
        self.ui.update_status(f"Search cancelled. {displayed} results shown.")
        self.logger.info(message)

    def _drain_remaining_results(self) -> tuple[str, object] | None:
        """Drain remaining results from the queue. Returns first non-result sentinel found."""
        try:
            while True:
                msg_type, data = self.result_queue.get_nowait()
                if msg_type == "result":
                    result: SearchResult = data  # type: ignore[assignment]
                    self.ui.add_result(  # type: ignore[union-attr]
                        result.file_path, result.display_text, result.formatted_mod_time
                    )
                else:
                    return (msg_type, data)
        except queue.Empty:
            return None

    def _cancel_search(self):
        """Cancel the current search operation."""
        self.cancel_event.set()
        self.ui.update_status("Search cancellation requested.")
        self.logger.info("Search cancellation requested")

    def _open_file(self, file_path: str):
        """Open a file from the results."""
        if not self.file_ops.open_file(file_path):
            self.ui.show_error_message("Error", f"Cannot open file: {file_path}")

    def _open_containing_folder(self, file_path: str):
        """Open the containing folder for a file."""
        if not self.file_ops.open_containing_folder(file_path):
            self.ui.show_error_message("Error", f"Cannot open containing folder for: {file_path}")

    def _export_results(self):
        """Handle export request."""
        self.ui.export_results()
        self.logger.info("Results exported")

    def run(self):
        """Start the application."""
        self.logger.info("Application started")
        self.ui.show()
