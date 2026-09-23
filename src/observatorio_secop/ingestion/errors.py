"""Expected, actionable errors from Bronze ingestion."""


class IngestionError(RuntimeError):
    """Base class for controlled ingestion failures."""


class IngestionConfigurationError(IngestionError):
    """Operational configuration or command arguments are invalid."""


class SourceContractError(IngestionError):
    """The source contract cannot safely govern ingestion."""


class TransportError(IngestionError):
    """A Socrata request failed permanently or exhausted its retries."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class PageValidationError(IngestionError):
    """A response page violates schema, size, or cursor guarantees."""


class ResumeError(IngestionError):
    """Persisted staging cannot be safely resumed."""


class ConcurrentRunError(IngestionError):
    """Another process already owns the ingestion lock."""
