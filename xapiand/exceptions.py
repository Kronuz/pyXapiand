class XapianError(Exception):
    pass


class AuthenticationError(XapianError):
    pass


class ServerError(XapianError):
    pass


class ConnectionError(ServerError):
    pass


class NewConnection(ConnectionError):
    pass


class InvalidIndexError(XapianError):
    """Raised when an index can not be opened."""
    pass
