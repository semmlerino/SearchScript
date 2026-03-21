#!/usr/bin/env python3
"""
Enhanced File Search Tool - Refactored Version

A GUI application for searching files and their contents with advanced filtering options.
Supports filename search and content search with file type filtering.
"""

import tkinter as tk
from search_controller import SearchController


def main():
    """Main application entry point."""
    root = tk.Tk()
    app = SearchController(root)
    app.run()


if __name__ == "__main__":
    main()