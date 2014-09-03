class XapianError(Exception):
    pass


class AuthenticationError(XapianError):
    pass


class ServerError(XapianError):
    pass


class ConnectionError(ServerError):
    pass


class InvalidIndexError(Exception):
    """Raised when an index can not be opened."""
    pass
