import csv
import json
import logging
from collections.abc import Callable

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
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
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)


class SearchUI(QMainWindow):
    def __init__(self, logger: logging.Logger | None = None):
        super().__init__()
        self.logger = logger or logging.getLogger(__name__)

        # Callbacks
        self.on_search_start: Callable | None = None
        self.on_search_cancel: Callable | None = None
        self.on_result_double_click: Callable | None = None
        self.on_open_containing_folder: Callable | None = None
        self.on_export: Callable | None = None

        # Search history
        self._search_history: list = []

        self._setup_ui()

    def _setup_ui(self):
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
        self._create_advanced_filters_row()
        self._create_progress_row()
        self._create_button_row()
        self._create_results_tree()

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
        self.mode_combo.addItems(["substring", "glob", "regex"])
        self.mode_combo.setCurrentText("substring")
        self.mode_combo.setFixedWidth(100)
        row.addWidget(self.mode_combo)

        row.addWidget(QLabel("Preset:"))
        self.preset_combo = QComboBox()
        self.preset_combo.addItems([
            "", "Images", "Code", "Documents", "Videos", "Archives", "Large Files (>10MB)"
        ])
        self.preset_combo.setFixedWidth(160)
        self.preset_combo.currentTextChanged.connect(self._apply_preset)
        row.addWidget(self.preset_combo)

        self._main_layout.addLayout(row)

    def _create_options_section(self):
        """Create checkbutton and file type filter rows."""
        self.within_checkbox = QCheckBox("Search within file contents")
        self._main_layout.addWidget(self.within_checkbox)

        include_row = QHBoxLayout()
        include_row.addWidget(QLabel("Include file types (e.g., .txt, .py):"))
        include_row.addStretch()
        self._main_layout.addLayout(include_row)

        self.include_entry = QLineEdit()
        self._main_layout.addWidget(self.include_entry)

        exclude_row = QHBoxLayout()
        exclude_row.addWidget(QLabel("Exclude file types (e.g., .log, .tmp):"))
        exclude_row.addStretch()
        self._main_layout.addLayout(exclude_row)

        self.exclude_entry = QLineEdit()
        self._main_layout.addWidget(self.exclude_entry)

    def _create_advanced_filters_row(self):
        """Create advanced filters row."""
        row = QHBoxLayout()

        row.addWidget(QLabel("Max depth:"))
        self.depth_entry = QLineEdit()
        self.depth_entry.setFixedWidth(60)
        row.addWidget(self.depth_entry)
        row.addSpacing(15)

        row.addWidget(QLabel("Min size (bytes):"))
        self.min_size_entry = QLineEdit()
        self.min_size_entry.setFixedWidth(100)
        row.addWidget(self.min_size_entry)
        row.addSpacing(15)

        row.addWidget(QLabel("Max size (bytes):"))
        self.max_size_entry = QLineEdit()
        self.max_size_entry.setFixedWidth(100)
        row.addWidget(self.max_size_entry)
        row.addSpacing(15)

        self.match_folders_checkbox = QCheckBox("Match folder names")
        row.addWidget(self.match_folders_checkbox)
        row.addStretch()

        self._main_layout.addLayout(row)

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
        self.search_button.setStyleSheet("background-color: blue; color: white;")
        self.search_button.clicked.connect(self._on_search_clicked)
        row.addWidget(self.search_button)

        self.cancel_button = QPushButton("Cancel")
        self.cancel_button.setFixedWidth(120)
        self.cancel_button.setStyleSheet("background-color: red; color: white;")
        self.cancel_button.clicked.connect(self._on_cancel_clicked)
        self.cancel_button.setEnabled(False)
        row.addWidget(self.cancel_button)

        self.export_button = QPushButton("Export")
        self.export_button.setFixedWidth(120)
        self.export_button.clicked.connect(self._on_export_clicked)
        self.export_button.setEnabled(False)
        row.addWidget(self.export_button)

        row.addStretch()
        self._main_layout.addLayout(row)

    def _create_results_tree(self):
        """Create results QTreeWidget."""
        columns = ["File Path", "Matching Line", "Last Modified"]
        self.results_tree = QTreeWidget()
        self.results_tree.setColumnCount(len(columns))
        self.results_tree.setHeaderLabels(columns)
        self.results_tree.setSortingEnabled(True)

        self.results_tree.setColumnWidth(0, 600)
        self.results_tree.setColumnWidth(1, 400)
        self.results_tree.setColumnWidth(2, 200)

        header = self.results_tree.header()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Interactive)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Interactive)

        # Double-click
        self.results_tree.itemDoubleClicked.connect(self._on_result_double_click)

        # Context menu
        self.results_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.results_tree.customContextMenuRequested.connect(self._show_context_menu)

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

        if self.on_search_start:
            include_text = self.include_entry.text()
            exclude_text = self.exclude_entry.text()
            search_params = {
                'directory': self.dir_entry.text(),
                'search_term': self.search_entry.text(),
                'include_types': [
                    ext.strip().lower() for ext in include_text.split(",")
                    if ext.strip()
                ],
                'exclude_types': [
                    ext.strip().lower() for ext in exclude_text.split(",")
                    if ext.strip()
                ],
                'search_within_files': self.within_checkbox.isChecked(),
                'search_mode': self.mode_combo.currentText(),
                'max_depth': (
                    int(self.depth_entry.text())
                    if self.depth_entry.text().strip() else None
                ),
                'min_size': (
                    int(self.min_size_entry.text())
                    if self.min_size_entry.text().strip() else None
                ),
                'max_size': (
                    int(self.max_size_entry.text())
                    if self.max_size_entry.text().strip() else None
                ),
                'match_folders': self.match_folders_checkbox.isChecked(),
            }
            self.on_search_start(search_params)

    def _on_cancel_clicked(self):
        """Handle cancel button click."""
        result = QMessageBox.question(
            self,
            "Cancel Search",
            "Are you sure you want to cancel the search?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if result == QMessageBox.StandardButton.Yes and self.on_search_cancel:
            self.on_search_cancel()

    def _validate_inputs(self) -> bool:
        """Validate user inputs."""
        if not self.dir_entry.text():
            QMessageBox.warning(self, "Input Error", "Please select a directory to search.")
            return False

        if not self.search_entry.text():
            QMessageBox.warning(self, "Input Error", "Please enter a search term.")
            return False

        return True

    def _on_result_double_click(self, item: QTreeWidgetItem, column: int):
        """Handle result double-click."""
        if self.on_result_double_click:
            file_path = item.text(0)
            self.on_result_double_click(file_path)

    def _show_context_menu(self, pos):
        """Show context menu on right-click."""
        item = self.results_tree.itemAt(pos)
        if item:
            self.results_tree.setCurrentItem(item)
            menu = QMenu(self)
            open_action = menu.addAction("Open Containing Folder")
            open_action.triggered.connect(self._on_open_containing_folder)
            menu.exec(self.results_tree.viewport().mapToGlobal(pos))

    def _on_open_containing_folder(self):
        """Handle open containing folder menu item."""
        if self.on_open_containing_folder:
            item = self.results_tree.currentItem()
            if item:
                file_path = item.text(0)
                self.on_open_containing_folder(file_path)

    def _apply_preset(self, preset: str):
        """Apply a file type preset."""
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

    def _on_export_clicked(self):
        if self.on_export:
            self.on_export()

    # --- Public API ---

    def set_search_state(self, searching: bool):
        """Update UI state for search/idle."""
        self.search_button.setEnabled(not searching)
        self.cancel_button.setEnabled(searching)

        if not searching:
            has_results = self.results_tree.topLevelItemCount() > 0
            self.export_button.setEnabled(has_results)
        else:
            self.export_button.setEnabled(False)

        if searching:
            self.progress_bar.setRange(0, 0)
        else:
            self.progress_bar.setRange(0, 1)
            self.progress_bar.setValue(0)

    def clear_results(self):
        """Clear the results tree."""
        self.results_tree.clear()

    def add_result(self, file_path: str, display_text: str = "", mod_time: str = ""):
        """Add a result row to the tree."""
        item = QTreeWidgetItem([file_path, display_text, mod_time])
        self.results_tree.addTopLevelItem(item)

    def update_status(self, message: str):
        """Update status label text."""
        self.status_label.setText(message)

    def show_no_results_message(self):
        """Show no results found message."""
        QMessageBox.information(self, "No Matches", "No matches found.")

    def show_error_message(self, title: str, message: str):
        """Show error message."""
        QMessageBox.critical(self, title, message)

    def export_results(self):
        """Export current results to JSON, CSV, or text."""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export Results",
            "",
            "JSON (*.json);;CSV (*.csv);;Text (*.txt)"
        )
        if not file_path:
            return

        rows = []
        for i in range(self.results_tree.topLevelItemCount()):
            item = self.results_tree.topLevelItem(i)
            if item is not None:
                rows.append((item.text(0), item.text(1), item.text(2)))

        if file_path.endswith('.json'):
            data = [{"file_path": r[0], "matching_line": r[1], "last_modified": r[2]} for r in rows]
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
        elif file_path.endswith('.csv'):
            with open(file_path, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(["File Path", "Matching Line", "Last Modified"])
                writer.writerows(rows)
        else:
            with open(file_path, 'w') as f:
                for r in rows:
                    f.write(f"{r[0]}\t{r[1]}\t{r[2]}\n")

        QMessageBox.information(self, "Export", f"Exported {len(rows)} results to {file_path}")

    def set_search_history(self, history: list):
        self._search_history = history

    def get_search_term(self) -> str:
        return self.search_entry.text()
