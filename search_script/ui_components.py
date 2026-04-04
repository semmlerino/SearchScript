import csv
import html
import json
import logging
import os
import re

from PySide6.QtCore import QPoint, QSettings, Qt, Signal
from PySide6.QtGui import QCloseEvent, QColor, QKeySequence, QShortcut
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDateEdit,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .search_engine import RAPIDFUZZ_AVAILABLE, SearchResult

SORT_ROLE = int(Qt.ItemDataRole.UserRole)
RESULT_ROLE = SORT_ROLE + 1


class ResultTreeWidgetItem(QTreeWidgetItem):
    """Tree item with numeric/date-aware sorting using stored sort keys."""

    def __lt__(self, other: QTreeWidgetItem) -> bool:
        tree = self.treeWidget()
        column = tree.sortColumn() if tree is not None else 0  # pyright: ignore[reportUnnecessaryComparison]
        left = self.data(column, SORT_ROLE)
        right = other.data(column, SORT_ROLE)
        if left is not None and right is not None:
            try:
                return left < right
            except TypeError:
                pass
        return super().__lt__(other)


class SearchUI(QMainWindow):
    search_requested = Signal(dict)  # emitted with search_params dict
    search_cancelled = Signal()
    refresh_requested = Signal()  # emitted when user requests cache-clear + re-search
    result_double_clicked = Signal(dict)  # emitted with serialized result dict
    open_folder_requested = Signal(str)  # emitted with file_path string

    def __init__(self, logger: logging.Logger | None = None):
        super().__init__()
        self.logger = logger or logging.getLogger(__name__)

        self._setup_ui()
        self._load_settings()

    def _load_settings(self) -> None:
        """Load user settings."""
        settings = QSettings("SearchScript", "EnhancedFileSearch")
        last_dir = settings.value("last_directory", "")
        last_search = settings.value("last_search_term", "")
        if isinstance(last_dir, str):
            self.dir_entry.setText(last_dir)
        if isinstance(last_search, str):
            self.search_entry.setText(last_search)
        mode_index = settings.value("search_mode_index")
        if isinstance(mode_index, int) and 0 <= mode_index < self.mode_combo.count():
            self.mode_combo.setCurrentIndex(mode_index)
        backend_index = settings.value("search_backend_index")
        if isinstance(backend_index, int) and 0 <= backend_index < self.backend_combo.count():
            self.backend_combo.setCurrentIndex(backend_index)
        within_files = settings.value("search_within_files")
        if isinstance(within_files, bool):
            self.within_checkbox.setChecked(within_files)
        geometry = settings.value("window_geometry")
        if geometry is not None:
            self.restoreGeometry(geometry)

    def closeEvent(self, event: QCloseEvent) -> None:
        """Save user settings on close."""
        settings = QSettings("SearchScript", "EnhancedFileSearch")
        settings.setValue("last_directory", self.dir_entry.text())
        settings.setValue("last_search_term", self.search_entry.text())
        settings.setValue("search_mode_index", self.mode_combo.currentIndex())
        settings.setValue("search_backend_index", self.backend_combo.currentIndex())
        settings.setValue("search_within_files", self.within_checkbox.isChecked())
        settings.setValue("window_geometry", self.saveGeometry())
        super().closeEvent(event)

    def _setup_ui(self) -> None:
        """Initialize the UI components."""
        self.setWindowTitle("Enhanced File Search Tool")
        self.resize(1200, 700)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self._main_layout = QVBoxLayout(central_widget)
        self._main_layout.setContentsMargins(10, 10, 10, 10)
        self._main_layout.setSpacing(6)

        self._create_directory_row()
        self._create_search_row()
        self._create_options_section()

        self._advanced_toggle = QPushButton("Show Advanced Filters")
        self._advanced_toggle.setCheckable(True)
        self._advanced_toggle.setChecked(False)
        self._advanced_toggle.clicked.connect(self._toggle_advanced_filters)
        self._main_layout.addWidget(self._advanced_toggle)

        self._advanced_container = QWidget()
        self._advanced_layout = QVBoxLayout(self._advanced_container)
        self._advanced_layout.setContentsMargins(0, 0, 0, 0)
        self._advanced_container.setVisible(False)
        self._main_layout.addWidget(self._advanced_container)

        self._create_advanced_filters_row()
        self._create_progress_row()
        self._create_button_row()
        self._create_results_tree()

        QShortcut(QKeySequence(Qt.Key.Key_F5), self, self._on_refresh_clicked)

    def _create_directory_row(self):
        """Create directory selection row."""
        row = QHBoxLayout()
        row.addWidget(QLabel("Directory:"))
        self.dir_entry = QLineEdit()
        row.addWidget(self.dir_entry, stretch=1)
        browse_btn = QPushButton("Browse")
        browse_btn.clicked.connect(self._browse_directory)
        row.addWidget(browse_btn)
        self._main_layout.addLayout(row)

    def _create_search_row(self):
        """Create search term row."""
        row = QHBoxLayout()
        row.addWidget(QLabel("Search Term:"))
        self.search_entry = QLineEdit()
        row.addWidget(self.search_entry, stretch=1)

        row.addWidget(QLabel("Mode:"))
        self.mode_combo = QComboBox()
        modes = ["substring", "glob", "regex"]
        if RAPIDFUZZ_AVAILABLE:
            modes.append("fuzzy")
        self.mode_combo.addItems(modes)
        self.mode_combo.setCurrentText("substring")
        self.mode_combo.setFixedWidth(100)
        row.addWidget(self.mode_combo)

        row.addWidget(QLabel("Backend:"))
        self.backend_combo = QComboBox()
        self.backend_combo.addItems(["auto", "python", "ripgrep"])
        self.backend_combo.setCurrentText("auto")
        self.backend_combo.setFixedWidth(100)
        row.addWidget(self.backend_combo)

        self.case_sensitive_checkbox = QCheckBox("Case sensitive")
        row.addWidget(self.case_sensitive_checkbox)

        row.addWidget(QLabel("Preset:"))
        self.preset_combo = QComboBox()
        self.preset_combo.addItems(
            ["", "Images", "Code", "Documents", "Videos", "Archives", "Large Files (>10MB)"]
        )
        self.preset_combo.setFixedWidth(160)
        self.preset_combo.currentTextChanged.connect(self._apply_preset)
        row.addWidget(self.preset_combo)

        self._main_layout.addLayout(row)

        self.search_entry.returnPressed.connect(self._on_search_clicked)

    def _create_options_section(self):
        """Create checkbutton and file type filter rows."""
        self.within_checkbox = QCheckBox("Search within file contents")
        self._main_layout.addWidget(self.within_checkbox)

        include_row = QHBoxLayout()
        include_row.addWidget(QLabel("Include file types (e.g., .txt, .py):"))
        include_row.addStretch()
        self._main_layout.addLayout(include_row)

        self.include_entry = QLineEdit()
        self.include_entry.setPlaceholderText(".txt, .py, .js")
        self._main_layout.addWidget(self.include_entry)

        exclude_row = QHBoxLayout()
        exclude_row.addWidget(QLabel("Exclude file types (e.g., .log, .tmp):"))
        exclude_row.addStretch()
        self._main_layout.addLayout(exclude_row)

        self.exclude_entry = QLineEdit()
        self.exclude_entry.setPlaceholderText(".log, .tmp")
        self._main_layout.addWidget(self.exclude_entry)

    def _create_advanced_filters_row(self) -> None:
        """Create advanced filters row."""
        row = QHBoxLayout()

        row.addWidget(QLabel("Max depth:"))
        self.depth_entry = QLineEdit()
        self.depth_entry.setPlaceholderText("unlimited")
        self.depth_entry.setFixedWidth(60)
        row.addWidget(self.depth_entry)
        row.addSpacing(15)

        row.addWidget(QLabel("Min size (bytes):"))
        self.min_size_entry = QLineEdit()
        self.min_size_entry.setPlaceholderText("e.g. 1048576")
        self.min_size_entry.setFixedWidth(100)
        row.addWidget(self.min_size_entry)
        row.addSpacing(15)

        row.addWidget(QLabel("Max size (bytes):"))
        self.max_size_entry = QLineEdit()
        self.max_size_entry.setPlaceholderText("no limit")
        self.max_size_entry.setFixedWidth(100)
        row.addWidget(self.max_size_entry)
        row.addSpacing(15)

        row.addWidget(QLabel("Max results:"))
        self.max_results_entry = QLineEdit()
        self.max_results_entry.setFixedWidth(90)
        self.max_results_entry.setPlaceholderText("optional")
        row.addWidget(self.max_results_entry)
        row.addSpacing(15)

        self.match_folders_checkbox = QCheckBox("Match folder names")
        row.addWidget(self.match_folders_checkbox)
        self.follow_symlinks_checkbox = QCheckBox("Follow symlinks")
        row.addWidget(self.follow_symlinks_checkbox)
        self.include_ignored_checkbox = QCheckBox("Include ignored files")
        self.include_ignored_checkbox.setChecked(False)
        row.addWidget(self.include_ignored_checkbox)
        row.addSpacing(15)

        row.addWidget(QLabel("Context:"))
        self.context_lines_spin = QSpinBox()
        self.context_lines_spin.setRange(0, 10)
        self.context_lines_spin.setValue(0)
        self.context_lines_spin.setFixedWidth(60)
        self.context_lines_spin.setToolTip("Number of context lines before/after matches")
        row.addWidget(self.context_lines_spin)
        row.addStretch()

        self._advanced_layout.addLayout(row)

        # Date filter row
        date_row = QHBoxLayout()

        date_row.addWidget(QLabel("Modified after:"))
        self.modified_after_entry: QDateEdit = QDateEdit()
        self.modified_after_entry.setCalendarPopup(True)
        self.modified_after_entry.setSpecialValueText(" ")
        self.modified_after_entry.setDate(self.modified_after_entry.minimumDate())
        self.modified_after_entry.setDisplayFormat("yyyy-MM-dd")
        date_row.addWidget(self.modified_after_entry)
        date_row.addSpacing(15)

        date_row.addWidget(QLabel("Modified before:"))
        self.modified_before_entry: QDateEdit = QDateEdit()
        self.modified_before_entry.setCalendarPopup(True)
        self.modified_before_entry.setSpecialValueText(" ")
        self.modified_before_entry.setDate(self.modified_before_entry.minimumDate())
        self.modified_before_entry.setDisplayFormat("yyyy-MM-dd")
        date_row.addWidget(self.modified_before_entry)
        date_row.addSpacing(15)

        self.clear_dates_btn: QPushButton = QPushButton("Clear dates")
        date_row.addWidget(self.clear_dates_btn)
        date_row.addStretch()

        self._advanced_layout.addLayout(date_row)

    def _create_progress_row(self):
        """Create progress bar and status label."""
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self._main_layout.addWidget(self.progress_bar)

        self.status_label = QLabel("")
        self._main_layout.addWidget(self.status_label)

    def _create_button_row(self):
        """Create search, cancel and export buttons."""
        row = QHBoxLayout()
        row.addStretch()

        self.search_button = QPushButton("Search")
        self.search_button.setFixedWidth(120)
        self.search_button.setDefault(True)
        self.search_button.setStyleSheet("background-color: blue; color: white;")
        self.search_button.clicked.connect(self._on_search_clicked)
        row.addWidget(self.search_button)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setFixedWidth(120)
        self.cancel_button.setStyleSheet("background-color: red; color: white;")
        self.cancel_button.clicked.connect(self._on_cancel_clicked)
        self.cancel_button.setEnabled(False)
        row.addWidget(self.cancel_button)

        self.refresh_button = QPushButton("Refresh")
        self.refresh_button.setFixedWidth(120)
        self.refresh_button.setToolTip("Clear cache and re-search (F5)")
        self.refresh_button.clicked.connect(self._on_refresh_clicked)
        self.refresh_button.setEnabled(False)
        row.addWidget(self.refresh_button)

        self.export_button = QPushButton("Export")
        self.export_button.setFixedWidth(120)
        self.export_button.clicked.connect(self.export_results)
        self.export_button.setEnabled(False)
        row.addWidget(self.export_button)

        row.addStretch()
        self._main_layout.addLayout(row)

    def _create_results_tree(self):
        """Create results QTreeWidget."""
        columns = ["File Path", "Matching Line", "Size", "Last Modified"]
        self.results_tree = QTreeWidget()
        self.results_tree.setColumnCount(len(columns))
        self.results_tree.setHeaderLabels(columns)
        self.results_tree.setSortingEnabled(True)

        self.results_tree.setColumnWidth(0, 500)
        self.results_tree.setColumnWidth(1, 400)
        self.results_tree.setColumnWidth(2, 100)
        self.results_tree.setColumnWidth(3, 150)

        header = self.results_tree.header()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Interactive)

        # Double-click
        self.results_tree.itemDoubleClicked.connect(self._on_result_double_click)

        # Context menu
        self.results_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.results_tree.customContextMenuRequested.connect(self._show_context_menu)  # pyright: ignore[reportUnknownMemberType,reportArgumentType]

        self._file_group_items: dict[str, ResultTreeWidgetItem] = {}

        self._main_layout.addWidget(self.results_tree, stretch=1)

    # --- Event handlers ---

    def _browse_directory(self):
        """Handle directory browse button click."""
        directory = QFileDialog.getExistingDirectory(self, "Select Directory")
        if directory:
            self.dir_entry.setText(directory)
            self.logger.info(f"Directory selected: {directory}")

    def _on_search_clicked(self):
        """Handle search button click."""
        if not self._validate_inputs():
            return

        include_text = self.include_entry.text()
        exclude_text = self.exclude_entry.text()
        search_params = {
            "directory": self.dir_entry.text(),
            "search_term": self.search_entry.text(),
            "include_types": [
                ext.strip().lower() for ext in include_text.split(",") if ext.strip()
            ],
            "exclude_types": [
                ext.strip().lower() for ext in exclude_text.split(",") if ext.strip()
            ],
            "search_within_files": self.within_checkbox.isChecked(),
            "search_mode": self.mode_combo.currentText(),
            "search_backend": self.backend_combo.currentText(),
            "max_depth": self._parse_optional_int(self.depth_entry.text()),
            "min_size": self._parse_optional_int(self.min_size_entry.text()),
            "max_size": self._parse_optional_int(self.max_size_entry.text()),
            "max_results": self._parse_optional_int(self.max_results_entry.text()),
            "match_folders": self.match_folders_checkbox.isChecked(),
            "follow_symlinks": self.follow_symlinks_checkbox.isChecked(),
            "include_ignored": self.include_ignored_checkbox.isChecked(),
            "context_lines": self.context_lines_spin.value(),
            "case_sensitive": self.case_sensitive_checkbox.isChecked(),
        }
        self.search_requested.emit(search_params)

    def _on_cancel_clicked(self):
        """Handle cancel button click."""
        self.search_cancelled.emit()

    def _on_refresh_clicked(self):
        """Handle refresh button / F5 shortcut."""
        self.refresh_requested.emit()

    def _validate_inputs(self) -> bool:
        """Validate user inputs."""
        directory = self.dir_entry.text().strip()
        if not directory:
            QMessageBox.warning(self, "Input Error", "Please select a directory to search.")
            return False
        if not os.path.isdir(directory):
            QMessageBox.warning(self, "Input Error", "Please select a valid directory.")
            return False

        search_term = self.search_entry.text().strip()
        if not search_term:
            QMessageBox.warning(self, "Input Error", "Please enter a search term.")
            return False

        if self.mode_combo.currentText() == "regex":
            try:
                re.compile(search_term)
            except re.error as exc:
                QMessageBox.warning(self, "Input Error", f"Invalid regular expression: {exc}")
                return False

        for label, widget in (
            ("Max depth", self.depth_entry),
            ("Min size", self.min_size_entry),
            ("Max size", self.max_size_entry),
        ):
            raw = widget.text().strip()
            if not raw:
                continue
            if not raw.isdigit():
                QMessageBox.warning(self, "Input Error", f"{label} must be a non-negative integer.")
                return False

        max_results_raw = self.max_results_entry.text().strip()
        if max_results_raw:
            if not max_results_raw.isdigit():
                QMessageBox.warning(
                    self,
                    "Input Error",
                    "Max results must be a positive integer.",
                )
                return False
            if int(max_results_raw) <= 0:
                QMessageBox.warning(
                    self,
                    "Input Error",
                    "Max results must be greater than zero.",
                )
                return False

        min_size = self._parse_optional_int(self.min_size_entry.text())
        max_size = self._parse_optional_int(self.max_size_entry.text())
        if min_size is not None and max_size is not None and min_size > max_size:
            QMessageBox.warning(self, "Input Error", "Min size cannot be greater than max size.")
            return False

        return True

    def _on_result_double_click(self, item: QTreeWidgetItem, column: int):
        """Handle result double-click."""
        result = item.data(0, RESULT_ROLE)
        if result is None:
            return
        if isinstance(result, dict) and result.get("is_group"):  # pyright: ignore[reportUnknownMemberType]
            item.setExpanded(not item.isExpanded())
            return
        self.result_double_clicked.emit(result)

    def _show_context_menu(self, pos: QPoint) -> None:
        """Show context menu on right-click."""
        item = self.results_tree.itemAt(pos)  # pyright: ignore[reportUnknownMemberType,reportArgumentType]
        if not item:
            return
        self.results_tree.setCurrentItem(item)
        menu = QMenu(self)
        result = item.data(0, RESULT_ROLE)  # pyright: ignore[reportUnknownMemberType]
        if isinstance(result, dict) and result.get("is_group"):  # pyright: ignore[reportUnknownMemberType]
            toggle_text = "Collapse" if item.isExpanded() else "Expand"
            toggle_action = menu.addAction(toggle_text)
            toggle_action.triggered.connect(lambda: item.setExpanded(not item.isExpanded()))
        open_action = menu.addAction("Open Containing Folder")
        open_action.triggered.connect(self._on_open_containing_folder)
        copy_path_action = menu.addAction("Copy File Path")
        copy_path_action.triggered.connect(self._copy_file_path)
        # Only show "Copy Matching Line" for non-group items with line_content
        if isinstance(result, dict) and not result.get("is_group") and result.get("line_content"):  # pyright: ignore[reportUnknownMemberType]
            copy_line_action = menu.addAction("Copy Matching Line")
            copy_line_action.triggered.connect(self._copy_matching_line)
        menu.exec(self.results_tree.viewport().mapToGlobal(pos))  # pyright: ignore[reportUnknownArgumentType,reportArgumentType]

    def _on_open_containing_folder(self):
        """Handle open containing folder menu item."""
        item = self.results_tree.currentItem()
        if not item:
            return
        result = item.data(0, RESULT_ROLE) or {}  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        if isinstance(result, dict) and result.get("is_group"):  # pyright: ignore[reportUnknownMemberType]
            file_path = result.get("file_path", item.text(0))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        else:
            file_path = result.get("file_path", item.text(0))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        self.open_folder_requested.emit(file_path)  # pyright: ignore[reportUnknownArgumentType]

    def _copy_file_path(self) -> None:
        """Copy the file path of the selected item to clipboard."""
        item = self.results_tree.currentItem()
        if not item:
            return
        result = item.data(0, RESULT_ROLE)  # pyright: ignore[reportUnknownMemberType]
        if isinstance(result, dict):
            file_path = result.get("file_path", item.text(0))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
        else:
            # Context line — get path from parent
            parent = item.parent()
            if parent is not None:  # pyright: ignore[reportUnnecessaryComparison]
                parent_result = parent.data(0, RESULT_ROLE)  # pyright: ignore[reportUnknownMemberType]
                if isinstance(parent_result, dict):
                    file_path = parent_result.get("file_path", parent.text(0))  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                else:
                    file_path = parent.text(0)
            else:
                file_path = item.text(0)
        QApplication.clipboard().setText(str(file_path))  # pyright: ignore[reportUnknownArgumentType]

    def _copy_matching_line(self) -> None:
        """Copy the matching line text to clipboard."""
        item = self.results_tree.currentItem()
        if not item:
            return
        result = item.data(0, RESULT_ROLE)  # pyright: ignore[reportUnknownMemberType]
        if isinstance(result, dict):
            line_content = result.get("line_content", "")  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
            if line_content:
                QApplication.clipboard().setText(str(line_content))  # pyright: ignore[reportUnknownArgumentType]

    def _apply_preset(self, preset: str) -> None:
        """Apply a file type preset."""
        # Clear stale advanced filter state
        self.min_size_entry.clear()
        self.max_size_entry.clear()
        self.depth_entry.clear()
        self.max_results_entry.clear()
        self.modified_after_entry.setDate(self.modified_after_entry.minimumDate())
        self.modified_before_entry.setDate(self.modified_before_entry.minimumDate())

        if not preset:
            self.include_entry.clear()
            self.exclude_entry.clear()
            return

        presets = {
            "Images": (".jpg, .jpeg, .png, .gif, .bmp, .tiff, .svg, .webp", ""),
            "Code": (".py, .js, .ts, .java, .cpp, .c, .h, .cs, .go, .rs, .rb, .sh", ""),
            "Documents": (".txt, .md, .pdf, .doc, .docx, .rtf, .odt", ""),
            "Videos": (".mp4, .avi, .mov, .mkv, .wmv, .flv, .webm", ""),
            "Archives": (".zip, .tar, .gz, .rar, .7z, .bz2, .xz", ""),
            "Large Files (>10MB)": ("", ""),
        }
        if preset in presets:
            inc, exc = presets[preset]
            self.include_entry.setText(inc)
            self.exclude_entry.setText(exc)
            if preset == "Large Files (>10MB)":
                self.min_size_entry.setText("10485760")
                self.include_entry.setText("")

    def _toggle_advanced_filters(self, checked: bool) -> None:
        """Toggle visibility of advanced filter options."""
        self._advanced_container.setVisible(checked)
        label = "Hide Advanced Filters" if checked else "Show Advanced Filters"
        self._advanced_toggle.setText(label)

    # --- Public API ---

    def set_search_state(self, searching: bool):
        """Update UI state for search/idle."""
        # Search button stays enabled so user can auto-restart with new params
        self.cancel_button.setEnabled(searching)

        if not searching:
            has_results = self.results_tree.topLevelItemCount() > 0
            self.export_button.setEnabled(has_results)
            self.refresh_button.setEnabled(has_results)
        else:
            self.export_button.setEnabled(False)
            self.refresh_button.setEnabled(False)

        if searching:
            self.progress_bar.setRange(0, 0)
        else:
            self.progress_bar.setRange(0, 1)
            self.progress_bar.setValue(0)

    def clear_results(self):
        """Clear the results tree."""
        self.results_tree.clear()
        self._file_group_items = {}

    def add_results_batch(self, items: list[SearchResult]) -> None:
        """Add multiple results, grouping content search results by file."""
        # Detect if this is content search (results have line numbers)
        is_content_search = any(r.line_number is not None for r in items)
        if not is_content_search:
            # Flat mode for filename search — unchanged behavior
            tree_items = [self._create_result_item(result) for result in items]
            self.results_tree.addTopLevelItems(tree_items)
            return
        # Grouped mode for content search
        for result in items:
            parent = self._file_group_items.get(result.file_path)
            if parent is None:
                parent = ResultTreeWidgetItem(
                    [
                        result.file_path,
                        "",  # match count placeholder, updated below
                        result.formatted_size,
                        result.formatted_mod_time,
                    ]
                )
                parent.setData(0, SORT_ROLE, result.file_path.lower())
                parent.setData(0, RESULT_ROLE, {"file_path": result.file_path, "is_group": True})
                parent.setData(
                    2, SORT_ROLE, result.file_size if result.file_size is not None else -1
                )
                parent.setData(
                    3, SORT_ROLE, result.mod_time if result.mod_time is not None else -1.0
                )
                parent.setToolTip(0, result.file_path)
                self._file_group_items[result.file_path] = parent
                self.results_tree.addTopLevelItem(parent)

            # Add before-context lines as dimmed children
            if result.context_before:
                for ctx_line in result.context_before:
                    ctx_item = QTreeWidgetItem(["", ctx_line, "", ""])
                    ctx_item.setForeground(1, QColor(128, 128, 128))
                    font = ctx_item.font(1)
                    font.setItalic(True)
                    ctx_item.setFont(1, font)
                    parent.addChild(ctx_item)

            # Add the match child
            child = self._create_result_item(result)
            parent.addChild(child)
            self._apply_match_highlight(child)

            # Add after-context lines as dimmed children
            if result.context_after:
                for ctx_line in result.context_after:
                    ctx_item = QTreeWidgetItem(["", ctx_line, "", ""])
                    ctx_item.setForeground(1, QColor(128, 128, 128))
                    font = ctx_item.font(1)
                    font.setItalic(True)
                    ctx_item.setFont(1, font)
                    parent.addChild(ctx_item)

            # Update match count — only count items with RESULT_ROLE data
            actual_matches = sum(
                1
                for j in range(parent.childCount())
                if parent.child(j) is not None and parent.child(j).data(0, RESULT_ROLE) is not None  # type: ignore[union-attr]
            )
            parent.setText(1, f"{actual_matches} match{'es' if actual_matches != 1 else ''}")
            parent.setData(1, SORT_ROLE, actual_matches)

    def _create_result_item(self, result: SearchResult) -> ResultTreeWidgetItem:
        """Convert a SearchResult into a sortable tree row with attached metadata."""
        item = ResultTreeWidgetItem(
            [
                result.file_path,
                result.display_text,
                result.formatted_size,
                result.formatted_mod_time,
            ]
        )
        metadata = self._serialize_result(result)
        item.setData(0, SORT_ROLE, result.file_path.lower())
        item.setData(0, RESULT_ROLE, metadata)
        item.setData(1, SORT_ROLE, result.line_number if result.line_number is not None else -1)
        item.setData(2, SORT_ROLE, result.file_size if result.file_size is not None else -1)
        item.setData(3, SORT_ROLE, result.mod_time if result.mod_time is not None else -1.0)
        item.setToolTip(0, result.file_path)
        if result.line_content:
            tooltip = result.line_content
            if result.next_line:
                tooltip = f"{tooltip}\n{result.next_line}"
            item.setToolTip(1, tooltip)
        # Store highlight info for widget creation
        if result.match_start is not None and result.match_length and result.match_length > 0:
            item.setData(1, SORT_ROLE + 1, (result.match_start, result.match_length))
        return item

    def _serialize_result(self, result: SearchResult) -> dict[str, str | int | float | None]:
        """Convert a SearchResult into exportable metadata."""
        return {
            "file_path": result.file_path,
            "line_number": result.line_number,
            "line_content": result.line_content,
            "next_line": result.next_line,
            "file_size": result.file_size,
            "mod_time": result.mod_time,
            "formatted_size": result.formatted_size,
            "formatted_mod_time": result.formatted_mod_time,
            "match_score": result.match_score,
            "match_start": result.match_start,
            "match_length": result.match_length,
        }

    def _apply_match_highlight(self, item: ResultTreeWidgetItem) -> None:
        """Apply HTML highlighting to the matching line column if match position is available."""
        highlight_data = item.data(1, SORT_ROLE + 1)
        if highlight_data is None:
            return
        match_start, match_length = highlight_data
        metadata = item.data(0, RESULT_ROLE)
        if not metadata:
            return
        line_content = metadata.get("line_content", "")
        line_number = metadata.get("line_number")
        if not line_content or line_number is None:
            return

        prefix = f"{line_number}: "
        before = html.escape(line_content[:match_start])
        matched = html.escape(line_content[match_start : match_start + match_length])
        after = html.escape(line_content[match_start + match_length :])
        html_text = f'{html.escape(prefix)}{before}<b style="color: #e8a025;">{matched}</b>{after}'

        label = QLabel(html_text)
        label.setTextFormat(Qt.TextFormat.RichText)
        label.setContentsMargins(2, 0, 2, 0)
        self.results_tree.setItemWidget(item, 1, label)

    def _parse_optional_int(self, value: str) -> int | None:
        """Parse an optional integer field after validation has run."""
        raw = value.strip()
        return int(raw) if raw else None

    def update_status(self, message: str):
        """Update status label text."""
        self.status_label.setText(message)

    def show_error_message(self, title: str, message: str):
        """Show error message."""
        QMessageBox.critical(self, title, message)

    def export_results(self):
        """Export current results to JSON, CSV, or text."""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Export Results", "", "JSON (*.json);;CSV (*.csv);;Text (*.txt)"
        )
        if not file_path:
            return

        rows: list[dict[str, str | int | float | None]] = []
        for i in range(self.results_tree.topLevelItemCount()):
            item = self.results_tree.topLevelItem(i)
            if item is None:
                continue
            if item.childCount() > 0:
                # Grouped parent — export children that have result metadata
                for j in range(item.childCount()):
                    child = item.child(j)
                    if child is not None:  # pyright: ignore[reportUnnecessaryComparison]
                        metadata = child.data(0, RESULT_ROLE)
                        if metadata:
                            rows.append(metadata)
            else:
                # Flat item
                metadata = item.data(0, RESULT_ROLE)
                if metadata:
                    rows.append(metadata)

        try:
            if file_path.endswith(".json"):
                with open(file_path, "w") as f:
                    json.dump(rows, f, indent=2)
            elif file_path.endswith(".csv"):
                with open(file_path, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow(
                        [
                            "File Path",
                            "Line Number",
                            "Matching Line",
                            "Next Line",
                            "Size Bytes",
                            "Last Modified",
                            "Match Score",
                        ]
                    )
                    for row in rows:
                        writer.writerow(
                            [
                                row["file_path"],
                                row["line_number"],
                                row["line_content"],
                                row["next_line"],
                                row["file_size"],
                                row["formatted_mod_time"],
                                row["match_score"],
                            ]
                        )
            else:
                with open(file_path, "w") as f:
                    for row in rows:
                        f.write(
                            f"{row['file_path']}\t{row['line_number'] or ''}\t"
                            f"{row['line_content'] or ''}\t{row['formatted_size']}\t"
                            f"{row['formatted_mod_time']}\n"
                        )
        except OSError as e:
            QMessageBox.critical(self, "Export Failed", f"Could not write to {file_path}:\n{e}")
            return

        self.update_status(f"Exported {len(rows)} results to {file_path}")

    def get_search_term(self) -> str:
        return self.search_entry.text()

    def get_result_summary(self) -> tuple[int, int]:
        """Return displayed match count and unique file count."""
        match_count = 0
        file_paths: set[str] = set()
        for index in range(self.results_tree.topLevelItemCount()):
            item = self.results_tree.topLevelItem(index)
            if item is None:
                continue
            if item.childCount() > 0:
                # Grouped parent — count only children with RESULT_ROLE data
                file_paths.add(item.text(0))
                for j in range(item.childCount()):
                    child = item.child(j)
                    if child is not None and child.data(0, RESULT_ROLE) is not None:  # pyright: ignore[reportUnnecessaryComparison]
                        match_count += 1
            else:
                # Flat item
                match_count += 1
                metadata = item.data(0, RESULT_ROLE) or {}  # pyright: ignore[reportUnknownMemberType,reportUnknownVariableType]
                file_paths.add(str(metadata.get("file_path", item.text(0))))  # pyright: ignore[reportUnknownMemberType,reportUnknownArgumentType]
        return match_count, len(file_paths)
