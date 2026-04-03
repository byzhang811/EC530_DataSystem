"""Custom exceptions used across the system."""

from collections.abc import Sequence


class DataSystemError(Exception):
    """Base exception for the project."""


class ValidationError(DataSystemError):
    """Raised when SQL validation fails."""

    def __init__(self, errors: Sequence[str]):
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))
