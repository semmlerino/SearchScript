"""Configuration and settings management for the search application."""


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
