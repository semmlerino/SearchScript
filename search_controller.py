import threading
import queue
import logging
from typing import Optional, Dict, Any
import tkinter as tk
from tkinter import messagebox

from search_engine import SearchEngine, SearchResult, SearchMode
from ui_components import SearchUI
from file_utils import FileOperations, LoggingConfig


class SearchController:
    """Main controller coordinating UI and search operations."""
    
    def __init__(self, root: tk.Tk):
        self.root = root
        self.logger = LoggingConfig.setup_logging()
        
        # Components
        self.search_engine = SearchEngine(self.logger)
        self.ui = SearchUI(root, self.logger)
        self.file_ops = FileOperations(self.logger)
        
        # Threading
        self.cancel_event = threading.Event()
        self.search_thread: Optional[threading.Thread] = None
        self.result_queue: Optional[queue.Queue] = None
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
    
    def _start_search(self, search_params: Dict[str, Any]):
        """Start the search operation."""
        self.logger.info(f"Starting search with parameters: {search_params}")

        # Track search history
        term = search_params['search_term']
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
            target=self._search_worker,
            args=(search_params,),
            daemon=True
        )
        self.search_thread.start()
        
        # Start monitoring results
        self.root.after(100, self._process_results)
    
    def _search_worker(self, search_params: Dict[str, Any]):
        """Worker thread for search operations."""
        try:
            results = []
            
            # Progress callback
            def progress_callback(message: str):
                self.result_queue.put(("status", message))
            
            # Convert search_mode string to enum
            mode_str = search_params.get('search_mode', 'substring')
            search_mode = SearchMode(mode_str)

            # Perform search
            for result in self.search_engine.search_files(
                directory=search_params['directory'],
                search_term=search_params['search_term'],
                include_types=search_params['include_types'],
                exclude_types=search_params['exclude_types'],
                search_within_files=search_params['search_within_files'],
                search_mode=search_mode,
                max_depth=search_params.get('max_depth'),
                min_size=search_params.get('min_size'),
                max_size=search_params.get('max_size'),
                match_folders=search_params.get('match_folders', False),
                progress_callback=progress_callback,
                cancel_event=self.cancel_event
            ):
                if self.cancel_event.is_set():
                    self.result_queue.put(("cancelled", f"Search cancelled. Found {len(results)} matches."))
                    return
                
                results.append(result)
            
            # Send completion message
            self.result_queue.put(("done", results))
            
        except Exception as e:
            error_msg = f"Search error: {str(e)}"
            self.result_queue.put(("error", error_msg))
            self.logger.error(error_msg)
    
    def _process_results(self):
        """Process results from the search thread."""
        try:
            while True:
                msg_type, data = self.result_queue.get_nowait()
                
                if msg_type == "done":
                    self._handle_search_complete(data)
                    return
                elif msg_type == "status":
                    self.ui.update_status(data)
                elif msg_type == "error":
                    self._handle_search_error(data)
                    return
                elif msg_type == "cancelled":
                    self._handle_search_cancelled(data)
                    return
                    
        except queue.Empty:
            pass
        
        # Continue monitoring if thread is alive
        if self.search_thread and self.search_thread.is_alive():
            self.root.after(100, self._process_results)
        else:
            # Thread finished without sending completion message
            self._handle_search_complete([])
    
    def _handle_search_complete(self, results: list):
        """Handle search completion."""
        self.ui.set_search_state(False)
        self.ui.update_status("Search completed.")
        
        if results:
            self._display_results(results)
            self.logger.info(f"Search completed with {len(results)} matches")
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
        self.ui.update_status(message)
        self.logger.info(message)
    
    def _display_results(self, results: list):
        """Display search results in the UI."""
        for result in results:
            mod_time = self.file_ops.get_file_modification_time(result.file_path)
            self.ui.add_result(result.file_path, result.display_text, mod_time)
        
        self.logger.info(f"Displayed {len(results)} results")
    
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
        self.root.mainloop()
        self.logger.info("Application closed")