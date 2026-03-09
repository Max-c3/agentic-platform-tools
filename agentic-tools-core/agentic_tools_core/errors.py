class PolicyError(Exception):
    """Raised when a tool call violates policy."""


class ScopeError(Exception):
    """Raised when a tool is outside routed scope."""


class RateLimitError(Exception):
    """Raised when a tool exceeds configured token bucket."""


class IdempotencyConflict(Exception):
    """Raised when idempotency key already exists for different payload."""


class ToolInputReferenceError(Exception):
    """Raised when a tool input $ref path cannot be resolved."""


class ToolVerificationError(Exception):
    """Raised when a tool output fails semantic verification."""

    def __init__(self, message: str, *, verification: dict | None = None, output: dict | None = None) -> None:
        super().__init__(message)
        self.verification = verification or {}
        self.output = output or {}
