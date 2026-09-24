"""Typed failures for the Silver to PostgreSQL boundary."""


class WarehouseError(Exception):
    """Base class for safe, operator-facing warehouse errors."""


class WarehouseConfigurationError(WarehouseError):
    """Warehouse configuration is incomplete or invalid."""


class SilverBundleError(WarehouseError):
    """The selected Silver bundle failed its trust checks."""


class WarehouseConflictError(WarehouseError):
    """A version identifier was reused for different immutable content."""


class WarehouseLoadError(WarehouseError):
    """PostgreSQL could not publish a verified Silver snapshot."""
