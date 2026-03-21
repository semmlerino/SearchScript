"""Configuration and settings management for the search application."""

import json
import os
from dataclasses import dataclass, asdict
from typing import List, Optional
import logging


@dataclass
class SearchConfig:
    """Configuration settings for the search application."""
    
    # Default search settings
    default_include_types: List[str] = None
    default_exclude_types: List[str] = None
    default_search_within_files: bool = False
    
    # UI settings
    window_width: int = 1200
    window_height: int = 700
    
    # Performance settings
    progress_update_interval: int = 10  # Update progress every N files
    max_line_length: int = 2000  # Truncate long lines

    # Search behavior
    default_search_mode: str = "substring"
    search_history: List[str] = None  # last 10 searches
    max_history_items: int = 10

    # Advanced filter defaults
    default_max_depth: Optional[int] = None
    default_match_folders: bool = False

    # Logging settings
    log_level: str = "INFO"
    log_file: str = "file_search.log"

    def __post_init__(self):
        if self.default_include_types is None:
            self.default_include_types = []
        if self.default_exclude_types is None:
            self.default_exclude_types = ['.log', '.tmp', '.cache']
        if self.search_history is None:
            self.search_history = []


class ConfigManager:
    """Manages application configuration."""
    
    def __init__(self, config_file: str = "search_config.json"):
        self.config_file = config_file
        self.config = self._load_config()
        self.logger = self._setup_logger()
    
    def _load_config(self) -> SearchConfig:
        """Load configuration from file or create default."""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    data = json.load(f)
                return SearchConfig(**data)
            except Exception as e:
                print(f"Error loading config: {e}. Using defaults.")
        
        # Return default config and save it
        config = SearchConfig()
        self.save_config(config)
        return config
    
    def save_config(self, config: Optional[SearchConfig] = None):
        """Save configuration to file."""
        if config is None:
            config = self.config
        
        try:
            with open(self.config_file, 'w') as f:
                json.dump(asdict(config), f, indent=2)
        except Exception as e:
            print(f"Error saving config: {e}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup logging based on configuration."""
        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        
        logging.basicConfig(
            filename=self.config.log_file,
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            filemode='a'
        )
        
        return logging.getLogger(__name__)
    
    def get_config(self) -> SearchConfig:
        """Get current configuration."""
        return self.config
    
    def update_config(self, **kwargs):
        """Update configuration parameters."""
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        self.save_config()


# Error handling classes
class SearchError(Exception):
    """Base exception for search operations."""
    pass


class DirectoryError(SearchError):
    """Exception for directory-related errors."""
    pass


class FileAccessError(SearchError):
    """Exception for file access errors."""
    pass


class ValidationError(SearchError):
    """Exception for input validation errors."""
    pass


# Global configuration instance
config_manager = ConfigManager()