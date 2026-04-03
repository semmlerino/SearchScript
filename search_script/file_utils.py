import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime


class FileOperations:
    """Utility class for file operations."""

    def __init__(self, logger: logging.Logger | None = None):
        self.logger = logger or logging.getLogger(__name__)

    def get_file_modification_time(self, file_path: str) -> str:
        """Get formatted modification time for a file."""
        try:
            mod_timestamp = os.path.getmtime(file_path)
            return datetime.fromtimestamp(mod_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        except OSError as e:
            self.logger.error(f"Error getting modification time for {file_path}: {e}")
            return "N/A"

    def open_file(self, file_path: str, line_number: int | None = None) -> bool:
        """Open file with default application."""
        if not os.path.exists(file_path):
            self.logger.warning(f"File does not exist: {file_path}")
            return False

        try:
            vscode = shutil.which("code")
            if line_number is not None and vscode:
                subprocess.call((vscode, "--goto", f"{file_path}:{line_number}"))
            elif sys.platform.startswith("darwin"):
                subprocess.call(("open", file_path))
            elif os.name == "nt":
                os.startfile(file_path)
            elif os.name == "posix":
                subprocess.call(("xdg-open", file_path))

            self.logger.info(f"Opened file: {file_path}")
            return True
        except (OSError, subprocess.SubprocessError) as e:
            self.logger.error(f"Error opening file {file_path}: {e}")
            return False

    def open_containing_folder(self, file_path: str) -> bool:
        """Open the folder containing the specified file."""
        if not os.path.exists(file_path):
            self.logger.warning(f"File does not exist: {file_path}")
            return False

        try:
            folder_path = os.path.dirname(file_path)

            if sys.platform.startswith("darwin"):
                subprocess.call(["open", folder_path])
                self.logger.info(f"Opened folder on macOS: {folder_path}")
            elif os.name == "nt":
                file_path_norm = os.path.normpath(file_path)
                command = f'explorer /select,"{file_path_norm}"'
                subprocess.Popen(command, shell=True)
                self.logger.info(
                    f"Executed Windows Explorer command to select file: {file_path_norm}"
                )
            elif os.name == "posix":
                subprocess.call(["xdg-open", folder_path])
                self.logger.info(f"Opened folder on POSIX system: {folder_path}")

            return True
        except (OSError, subprocess.SubprocessError) as e:
            self.logger.error(f"Error opening containing folder for {file_path}: {e}")
            return False


class LoggingConfig:
    """Configuration for logging setup."""

    @staticmethod
    def setup_logging(
        log_file: str = "file_search.log", level: int = logging.DEBUG
    ) -> logging.Logger:
        """Setup logging configuration."""
        logging.basicConfig(
            filename=log_file,
            level=level,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filemode="a",
        )

        logger = logging.getLogger(__name__)
        logger.info("Logging initialized")
        return logger
