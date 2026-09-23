"""Domain errors for Silver processing."""


class SilverError(Exception):
    """Base error shown by the Silver command."""


class SilverConfigurationError(SilverError):
    """The Silver configuration or territorial catalog is invalid."""


class BronzeEvidenceError(SilverError):
    """Committed Bronze evidence is missing, altered, or incompatible."""


class QualityGateError(SilverError):
    """A candidate failed one or more critical quality checks."""


class PublicationError(SilverError):
    """A Silver bundle cannot be safely staged or promoted."""
