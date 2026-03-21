import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import logging
import json
import csv
from typing import Callable, Optional
from datetime import datetime


class SearchUI:
    def __init__(self, root: tk.Tk, logger: Optional[logging.Logger] = None):
        self.root = root
        self.logger = logger or logging.getLogger(__name__)
        
        # Variables
        self.dir_var = tk.StringVar()
        self.search_var = tk.StringVar()
        self.include_var = tk.StringVar()
        self.exclude_var = tk.StringVar()
        self.within_var = tk.BooleanVar()
        self.status_var = tk.StringVar()
        
        self.mode_var = tk.StringVar(value="substring")
        self.depth_var = tk.StringVar()
        self.min_size_var = tk.StringVar()
        self.max_size_var = tk.StringVar()
        self.match_folders_var = tk.BooleanVar()

        # Callbacks
        self.on_search_start: Optional[Callable] = None
        self.on_search_cancel: Optional[Callable] = None
        self.on_result_double_click: Optional[Callable] = None
        self.on_open_containing_folder: Optional[Callable] = None
        self.on_export: Optional[Callable] = None

        # Search history
        self._search_history: list = []
        
        self._setup_ui()
    
    def _setup_ui(self):
        """Initialize the UI components."""
        self.root.title("Enhanced File Search Tool")
        self.root.geometry("1200x700")
        self.root.resizable(True, True)
        
        self._create_directory_frame()
        self._create_search_frame()
        self._create_options_frame()
        self._create_progress_frame()
        self._create_button_frame()
        self._create_results_frame()
    
    def _create_directory_frame(self):
        """Create directory selection frame."""
        dir_frame = tk.Frame(self.root)
        dir_frame.pack(pady=10, padx=10, fill='x')
        
        tk.Label(dir_frame, text="Directory:").pack(side=tk.LEFT)
        tk.Entry(dir_frame, textvariable=self.dir_var, width=80).pack(side=tk.LEFT, padx=5)
        tk.Button(dir_frame, text="Browse", command=self._browse_directory).pack(side=tk.LEFT)
    
    def _create_search_frame(self):
        """Create search term frame."""
        search_frame = tk.Frame(self.root)
        search_frame.pack(pady=5, padx=10, fill='x')
        
        tk.Label(search_frame, text="Search Term:").pack(side=tk.LEFT)
        self.search_entry = tk.Entry(search_frame, textvariable=self.search_var, width=80)
        self.search_entry.pack(side=tk.LEFT, padx=5)

        # Search mode
        tk.Label(search_frame, text="Mode:").pack(side=tk.LEFT, padx=(15, 0))
        self.mode_combo = ttk.Combobox(
            search_frame, textvariable=self.mode_var,
            values=["substring", "glob", "regex"], state="readonly", width=10
        )
        self.mode_combo.pack(side=tk.LEFT, padx=5)

        # Presets
        tk.Label(search_frame, text="Preset:").pack(side=tk.LEFT, padx=(15, 0))
        self.preset_combo = ttk.Combobox(
            search_frame, values=[
                "", "Images", "Code", "Documents", "Videos", "Archives", "Large Files (>10MB)"
            ], state="readonly", width=18
        )
        self.preset_combo.pack(side=tk.LEFT, padx=5)
        self.preset_combo.bind("<<ComboboxSelected>>", self._apply_preset)
    
    def _create_options_frame(self):
        """Create options frame."""
        options_frame = tk.Frame(self.root)
        options_frame.pack(pady=5, padx=10, fill='x')
        
        tk.Checkbutton(
            options_frame, 
            text="Search within file contents", 
            variable=self.within_var
        ).pack(anchor='w')
        
        tk.Label(
            options_frame, 
            text="Include file types (e.g., .txt, .py):"
        ).pack(anchor='w', pady=(10, 0))
        tk.Entry(
            options_frame, 
            textvariable=self.include_var, 
            width=100
        ).pack(anchor='w', padx=5)
        
        tk.Label(
            options_frame, 
            text="Exclude file types (e.g., .log, .tmp):"
        ).pack(anchor='w', pady=(10, 0))
        tk.Entry(
            options_frame,
            textvariable=self.exclude_var,
            width=100
        ).pack(anchor='w', padx=5)

        # Advanced filters row
        adv_frame = tk.Frame(options_frame)
        adv_frame.pack(anchor='w', pady=(10, 0))

        tk.Label(adv_frame, text="Max depth:").pack(side=tk.LEFT)
        tk.Entry(adv_frame, textvariable=self.depth_var, width=6).pack(side=tk.LEFT, padx=(5, 15))

        tk.Label(adv_frame, text="Min size (bytes):").pack(side=tk.LEFT)
        tk.Entry(adv_frame, textvariable=self.min_size_var, width=12).pack(side=tk.LEFT, padx=(5, 15))

        tk.Label(adv_frame, text="Max size (bytes):").pack(side=tk.LEFT)
        tk.Entry(adv_frame, textvariable=self.max_size_var, width=12).pack(side=tk.LEFT, padx=(5, 15))

        tk.Checkbutton(adv_frame, text="Match folder names", variable=self.match_folders_var).pack(side=tk.LEFT, padx=(15, 0))
    
    def _create_progress_frame(self):
        """Create progress bar and status."""
        progress_frame = tk.Frame(self.root)
        progress_frame.pack(pady=10, padx=10, fill='x')
        
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.pack(fill='x')
        
        tk.Label(self.root, textvariable=self.status_var).pack(pady=(0, 10))
    
    def _create_button_frame(self):
        """Create search and cancel buttons."""
        button_frame = tk.Frame(self.root)
        button_frame.pack(pady=5)
        
        self.search_button = tk.Button(
            button_frame, 
            text="Search", 
            command=self._on_search_clicked,
            bg="blue", 
            fg="white", 
            width=15
        )
        self.search_button.pack(side=tk.LEFT, padx=10)
        
        self.cancel_button = tk.Button(
            button_frame, 
            text="Cancel", 
            command=self._on_cancel_clicked,
            bg="red", 
            fg="white", 
            width=15, 
            state=tk.DISABLED
        )
        self.cancel_button.pack(side=tk.LEFT, padx=10)

        self.export_button = tk.Button(
            button_frame, text="Export", command=self._on_export_clicked,
            width=15, state=tk.DISABLED
        )
        self.export_button.pack(side=tk.LEFT, padx=10)
    
    def _create_results_frame(self):
        """Create results treeview."""
        results_frame = tk.Frame(self.root)
        results_frame.pack(pady=5, padx=10, fill='both', expand=True)
        
        columns = ("File Path", "Matching Line", "Last Modified")
        self.results_tree = ttk.Treeview(results_frame, columns=columns, show='headings')
        
        # Configure columns
        for col in columns:
            self.results_tree.heading(
                col, 
                text=col, 
                command=lambda c=col: self._sort_column(c, False)
            )
        
        self.results_tree.column("File Path", width=600)
        self.results_tree.column("Matching Line", width=400)
        self.results_tree.column("Last Modified", width=200)
        self.results_tree.pack(side='left', fill='both', expand=True)
        
        # Scrollbar
        scrollbar = ttk.Scrollbar(results_frame, orient="vertical", command=self.results_tree.yview)
        self.results_tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side='right', fill='y')
        
        # Event bindings
        self.results_tree.bind("<Double-1>", self._on_result_double_click)
        self.results_tree.bind("<Button-3>", self._show_context_menu)
        
        # Context menu
        self.context_menu = tk.Menu(self.root, tearoff=0)
        self.context_menu.add_command(
            label="Open Containing Folder", 
            command=self._on_open_containing_folder
        )
    
    def _browse_directory(self):
        """Handle directory browse button click."""
        directory = filedialog.askdirectory()
        if directory:
            self.dir_var.set(directory)
            self.logger.info(f"Directory selected: {directory}")
    
    def _on_search_clicked(self):
        """Handle search button click."""
        if not self._validate_inputs():
            return
        
        if self.on_search_start:
            search_params = {
                'directory': self.dir_var.get(),
                'search_term': self.search_entry.get(),
                'include_types': [ext.strip().lower() for ext in self.include_var.get().split(",") if ext.strip()],
                'exclude_types': [ext.strip().lower() for ext in self.exclude_var.get().split(",") if ext.strip()],
                'search_within_files': self.within_var.get(),
                'search_mode': self.mode_var.get(),
                'max_depth': int(self.depth_var.get()) if self.depth_var.get().strip() else None,
                'min_size': int(self.min_size_var.get()) if self.min_size_var.get().strip() else None,
                'max_size': int(self.max_size_var.get()) if self.max_size_var.get().strip() else None,
                'match_folders': self.match_folders_var.get(),
            }
            self.on_search_start(search_params)
    
    def _on_cancel_clicked(self):
        """Handle cancel button click."""
        if messagebox.askyesno("Cancel Search", "Are you sure you want to cancel the search?"):
            if self.on_search_cancel:
                self.on_search_cancel()
    
    def _validate_inputs(self) -> bool:
        """Validate user inputs."""
        if not self.dir_var.get():
            messagebox.showwarning("Input Error", "Please select a directory to search.")
            return False
        
        if not self.search_entry.get():
            messagebox.showwarning("Input Error", "Please enter a search term.")
            return False
        
        return True
    
    def _sort_column(self, col: str, reverse: bool):
        """Sort treeview column."""
        try:
            items = [(self.results_tree.set(k, col), k) for k in self.results_tree.get_children('')]
            
            if col == "Last Modified":
                items.sort(
                    key=lambda t: datetime.strptime(t[0], "%Y-%m-%d %H:%M:%S") if t[0] != "" else datetime.min, 
                    reverse=reverse
                )
            else:
                items.sort(key=lambda t: t[0].lower(), reverse=reverse)
            
            for index, (val, k) in enumerate(items):
                self.results_tree.move(k, '', index)
            
            self.results_tree.heading(col, command=lambda: self._sort_column(col, not reverse))
        except Exception as e:
            self.logger.error(f"Error sorting column {col}: {e}")
    
    def _on_result_double_click(self, event):
        """Handle result double-click."""
        if self.on_result_double_click:
            selected_item = self.results_tree.selection()
            if selected_item:
                item = self.results_tree.item(selected_item)
                file_path = item['values'][0]
                self.on_result_double_click(file_path)
    
    def _show_context_menu(self, event):
        """Show context menu on right-click."""
        selected_item = self.results_tree.identify_row(event.y)
        if selected_item:
            self.results_tree.selection_set(selected_item)
            self.context_menu.post(event.x_root, event.y_root)
    
    def _on_open_containing_folder(self):
        """Handle open containing folder menu item."""
        if self.on_open_containing_folder:
            selected_item = self.results_tree.selection()
            if selected_item:
                item = self.results_tree.item(selected_item)
                file_path = item['values'][0]
                self.on_open_containing_folder(file_path)
    
    def set_search_state(self, searching: bool):
        """Update UI state for search/idle."""
        self.search_button.config(state=tk.DISABLED if searching else tk.NORMAL)
        self.cancel_button.config(state=tk.NORMAL if searching else tk.DISABLED)
        if not searching:
            has_results = len(self.results_tree.get_children()) > 0
            self.export_button.config(state=tk.NORMAL if has_results else tk.DISABLED)
        else:
            self.export_button.config(state=tk.DISABLED)

        if searching:
            self.progress_bar.config(mode='indeterminate')
            self.progress_bar.start(10)
        else:
            self.progress_bar.stop()
            self.progress_bar.config(mode='determinate', value=0)
    
    def clear_results(self):
        """Clear the results tree."""
        self.results_tree.delete(*self.results_tree.get_children())
    
    def add_result(self, file_path: str, display_text: str = "", mod_time: str = ""):
        """Add a result to the tree."""
        self.results_tree.insert("", tk.END, values=(file_path, display_text, mod_time))
    
    def update_status(self, message: str):
        """Update status message."""
        self.status_var.set(message)
    
    def show_no_results_message(self):
        """Show no results found message."""
        messagebox.showinfo("No Matches", "No matches found.")
    
    def show_error_message(self, title: str, message: str):
        """Show error message."""
        messagebox.showerror(title, message)

    def _apply_preset(self, event=None):
        preset = self.preset_combo.get()
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
            self.include_var.set(inc)
            self.exclude_var.set(exc)
            if preset == "Large Files (>10MB)":
                self.min_size_var.set("10485760")
                self.include_var.set("")

    def _on_export_clicked(self):
        if self.on_export:
            self.on_export()

    def export_results(self):
        """Export current results to JSON or CSV."""
        file_path = filedialog.asksaveasfilename(
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("CSV", "*.csv"), ("Text", "*.txt")]
        )
        if not file_path:
            return

        items = self.results_tree.get_children()
        rows = [self.results_tree.item(item)['values'] for item in items]

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

        messagebox.showinfo("Export", f"Exported {len(rows)} results to {file_path}")

    def set_search_history(self, history: list):
        self._search_history = history

    def get_search_term(self) -> str:
        return self.search_var.get()