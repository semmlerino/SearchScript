#!/usr/bin/env python3
"""Enhanced File Search Tool - PySide6 GUI application."""

import sys

from PySide6.QtWidgets import QApplication

from .search_controller import SearchController


def main():
    """Main application entry point."""
    app = QApplication(sys.argv)
    controller = SearchController()
    app.aboutToQuit.connect(controller.search_engine.shutdown)
    controller.ui.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
