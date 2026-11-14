"""Stable export failures with no source values or connection details."""


class ExportError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.safe_details: dict[str, object] = {}
        self.code = code
        super().__init__(message)
