class ProvisioningError(Exception):
    """A bounded, redacted error suitable for status and audit records."""

    def __init__(self, code, retryable=False, retry_after=60, status=None):
        super().__init__(code)
        self.code = code
        self.retryable = retryable
        self.retry_after = max(1, min(int(retry_after), 86400))
        self.status = status
