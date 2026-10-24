"""Safe publication errors with stable machine-readable codes."""


class PublicationError(ValueError):
    """A rejected publication operation; messages must not include credentials."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)
