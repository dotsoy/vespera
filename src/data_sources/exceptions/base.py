"""
Base exceptions for the data sources module.

This module defines the core exception hierarchy for the data sources package.
All custom exceptions should inherit from DataSourceError.
"""
from typing import Optional, Dict, Any, Type, TypeVar, Union, List, Tuple, Callable, Set


class DataSourceError(Exception):
    """Base class for all data source related exceptions."""
    pass


class DataSourceNotAvailableError(DataSourceError):
    """Raised when a data source is not available or accessible."""
    pass


class DataSourceConfigurationError(DataSourceError):
    """Raised when there is an error in the data source configuration."""
    pass


class DataSourceInitializationError(DataSourceError):
    """Raised when a data source fails to initialize."""
    pass


class DataSourceConnectionError(DataSourceError):
    """Raised when there is a connection error with a data source."""
    pass


class DataSourceAuthenticationError(DataSourceError):
    """Raised when authentication with a data source fails."""
    pass


class DataSourceRateLimitError(DataSourceError):
    """Raised when a rate limit is exceeded for a data source."""
    pass


class DataSourceRequestError(DataSourceError):
    """Raised when there is an error with a request to a data source."""
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 response_text: Optional[str] = None):
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(message)


class DataSourceResponseError(DataSourceError):
    """Raised when there is an error in the response from a data source."""
    pass


class DataSourceNotFoundError(DataSourceError):
    """Raised when a requested data source is not found."""
    pass


class DataSourceNotSupportedError(DataSourceError):
    """Raised when a requested operation is not supported by the data source."""
    pass


class DataSourceValidationError(DataSourceError):
    """Raised when there is a validation error with data from a data source."""
    pass


class DataSourceTimeoutError(DataSourceError):
    """Raised when a request to a data source times out."""
    pass


class DataSourceDeprecatedError(DataSourceError):
    """Raised when a data source is deprecated and no longer available."""
    pass


class DataSourceMaintenanceError(DataSourceError):
    """Raised when a data source is under maintenance."""
    pass


class DataSourceInsufficientFundsError(DataSourceError):
    """Raised when there are insufficient funds to access a paid data source."""
    pass


class DataSourceSubscriptionExpiredError(DataSourceError):
    """Raised when a subscription to a paid data source has expired."""
    pass
