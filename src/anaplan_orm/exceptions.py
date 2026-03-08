class AnaplanORMError(Exception):
    """Base exception for all errors raised by anaplan-orm."""
    pass

class AnaplanConnectionError(AnaplanORMError):
    """Raised when the client cannot connect to the Anaplan REST API (e.g., SSL/Network errors)."""
    pass