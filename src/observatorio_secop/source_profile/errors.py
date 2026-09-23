"""Actionable errors raised by the source profiling workflow."""


class SourceProfileError(RuntimeError):
    """Base error for an expected and actionable profiling failure."""


class ConfigurationError(SourceProfileError):
    """The local source configuration is missing or unsafe."""


class QueryLimitError(SourceProfileError):
    """A query exceeds the configured request or response budget."""


class SourceUnavailableError(SourceProfileError):
    """The public source could not be reached after bounded retries."""


class InvalidResponseError(SourceProfileError):
    """The source returned empty, invalid, or inconsistent content."""


class ContractValidationError(SourceProfileError):
    """Observed records do not satisfy the frozen source contract."""
