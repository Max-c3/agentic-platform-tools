class IntegrationConfigError(RuntimeError):
    """Raised when integration client config is missing or invalid."""


class IntegrationRequestError(RuntimeError):
    """Raised when integration API request fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int = None,
        method: str = None,
        url: str = None,
        response_text: str = None,
        response_json=None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.method = method
        self.url = url
        self.response_text = response_text
        self.response_json = response_json
