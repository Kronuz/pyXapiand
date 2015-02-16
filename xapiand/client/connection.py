from __future__ import absolute_import, unicode_literals, print_function

import sys
import time
import Queue
import socket
import weakref
import contextlib

from errno import EISCONN, EINVAL, ECONNREFUSED
from functools import wraps

from ..parser import SPLIT_RE
from ..exceptions import ConnectionError, NewConnection
from ..utils import sendall, readline


# Sentinel used to mark an empty slot in the ConnectionPool queue.
# Using sys.maxint as the timestamp ensures that empty slots will always
# sort *after* live connection objects in the queue.
EMPTY_SLOT = (sys.maxint, None)


def command(func=False, **kwargs):
    def _command(func):
        func.command = func.__name__
        for attr, value in kwargs.items():
            setattr(func, attr, value)
        return func
    if callable(func):
        return _command(func)
    return _command


class ConnectionPool(object):
    def __init__(self, server, maxsize=None, max_age=60,
                 wait_for_connection=None):
        self.server = weakref.ref(server)
        self.maxsize = maxsize
        self.max_age = max_age
        self.clients = Queue.PriorityQueue(maxsize)
        self.wait_for_connection = wait_for_connection
        # If there is a maxsize, prime the queue with empty slots.
        if maxsize is not None:
            for _ in xrange(maxsize):
                self.clients.put(EMPTY_SLOT)

    def factory(self):
        return self.server().factory()

    def checkout(self):
        ts, connection = self._checkout_connection()
        connection._checkout = (self, ts)
        return connection

    def _checkout_connection(self):
        # If there's no maxsize, no need to block waiting for a connection.
        blocking = self.maxsize is not None
        # Loop until we get a non-stale connection, or we create a new one.
        while True:
            try:
                ts, connection = self.clients.get(blocking, self.wait_for_connection)
            except Queue.Empty:
                if blocking:
                    # timeout
                    raise Exception("No connections available in the pool")
                else:
                    # No maxsize and no free connections, create a new one.
                    # XXX TODO: we should be using a monotonic clock here.
                    now = time.time()
                    connection = self.factory()
                    return now, connection
            else:
                now = time.time()
                # If we got an empty slot placeholder, create a new connection.
                if connection is None:
                    try:
                        connection = self.factory()
                        return now, connection
                    except Exception:
                        if self.maxsize is not None:
                            # return slot to queue
                            self.clients.put(EMPTY_SLOT)
                        raise
                # If the connection is not stale, go ahead and use it.
                if self.max_age is None or now - ts < self.max_age:
                    return ts, connection
                # Otherwise, the connection is stale.
                # Close it, push an empty slot onto the queue, and retry.
                connection.disconnect()
                self.clients.put(EMPTY_SLOT)
                continue

    def _checkin_connection(self, ts, connection):
        """Return a connection to the pool."""
        # If the connection is now stale, don't return it to the pool.
        # Push an empty slot instead so that it will be refreshed when needed.
        now = time.time()
        if connection.client_socket and (self.max_age is None or now - ts < self.max_age):
            self.clients.put((ts, connection))
        else:
            if self.maxsize is not None:
                self.clients.put(EMPTY_SLOT)


def with_retry(func):
    @wraps(func)
    def _with_retry(self, *args, **kw):
        retries = 0
        delay = self.reconnect_delay

        while retries <= self.max_connect_retries:
            try:
                return func(self, *args, **kw)
            except NewConnection:
                continue
            except (IOError, RuntimeError, socket.error, ConnectionError):
                exc_info = sys.exc_info()
                self.sleep(delay)
                retries += 1
                delay *= 3  # growing the delay

        raise exc_info[0], exc_info[1], exc_info[2]
    return _with_retry


class Connection(object):
    MAX_READ_LENGTH = 1000000
    delimiter = '\r\n'

    def __init__(self, host='localhost', port=8890, endpoints=None,
                 max_connect_retries=5, reconnect_delay=0.1,
                 socket_timeout=4, encoding='utf-8', encoding_errors='strict',
                 socket_class=socket.socket, sleep=time.sleep):
        self.socket_class = socket_class
        self.sleep = sleep
        self.host = host
        self.port = port
        self.endpoints = endpoints
        self.max_connect_retries = max_connect_retries
        self.reconnect_delay = reconnect_delay
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.client_socket = None
        self.cmd_id = 0

    def __del__(self):
        try:
            self.disconnect()
            self.checkin()
        except Exception:
            pass

    def checkin(self):
        if self._checkout:
            pool, ts = self._checkout
            self._checkout = None
            self._client_responses_socket = None
            self.cmd_id += 1
            pool._checkin_connection(ts, self)

    def _error_message(self, exception):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            return "Error connecting to %s:%s. %s." % \
                (self.host, self.port, exception.args[0])
        else:
            return "Error %s connecting %s:%s. %s." % \
                (exception.args[0], self.host, self.port, exception.args[1])

    def on_connect(self):
        pass

    def on_disconnect(self):
        pass

    @property
    def client_responses(self):
        if not hasattr(self, '_client_responses') or getattr(self, '_client_responses_socket', None) != self.client_socket:
            self._client_responses_socket = self.client_socket
            self._client_responses = readline(self._client_responses_socket)
        return self._client_responses

    def connect(self):
        if not self.client_socket:
            try:
                sock = self._connect()
            except socket.error:
                exc_info = sys.exc_info()
                e = exc_info[1]
                raise ConnectionError(self._error_message(e)), None, exc_info[2]
            self.client_socket = sock
        self.cmd_id = 0
        self.on_connect()

    def _connect(self):
        "Create a TCP socket connection"
        sock = self.socket_class(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)

        retries = 0
        delay = self.reconnect_delay

        while retries <= self.max_connect_retries:
            try:
                sock.connect((self.host, self.port))
                return sock
            except socket.error as exc:
                exc_info = sys.exc_info()
                if exc.errno == EISCONN:
                    return sock   # we're good
                if exc.errno == EINVAL:
                    # we're doomed, recreate socket
                    sock = self.socket_class(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(self.socket_timeout)
                self.sleep(delay)
                retries += 1
                delay *= 3  # growing the delay

        raise exc_info[0], exc_info[1], exc_info[2]

    def disconnect(self):
        "Disconnects from the server"
        self.on_disconnect()
        self.cmd_id = 0
        client_socket, self.client_socket = self.client_socket, None
        if client_socket is not None:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
                client_socket.close()
            except socket.error:
                pass

    def send(self, body):
        if not self.client_socket:
            self.connect()
            raise NewConnection("New connection made!")
        # print('<<<<---', id(self), '%s:%s' % (self.address[0], self.address[1]), repr(body), file=sys.stderr)
        try:
            sendall(self.client_socket, body)
        except socket.error:
            self.disconnect()
            exc_info = sys.exc_info()
            e = exc_info[1]
            if len(e.args) == 1:
                _errno, errmsg = "UNKNOWN", e.args[0]
            else:
                _errno, errmsg = e.args
            raise ConnectionError("Error %s while writing to socket. %s." % (_errno, errmsg)), None, exc_info[2]
        except Exception:
            self.disconnect()
            raise

    def read(self):
        "Read the response from a previously sent command"
        cmd_id = self.cmd_id
        for response in self.client_responses:
            if not response:
                self.disconnect()
                raise ConnectionError("No response!")
            # print('--->>>>', id(self), '%s:%s' % (self.address[0], self.address[1]), repr(response), file=sys.stderr)
            response = response[:-2]
            if response:
                if response[0] in (b"#", b" "):
                    continue
                try:
                    _cmd_id, response = response.split(b'. ', 1)
                    _cmd_id = int(_cmd_id)
                except ValueError:
                    self.disconnect()
                    raise ConnectionError("Received a wrong response from the server: %r" % response)
                if _cmd_id != cmd_id:
                    if _cmd_id < cmd_id:
                        continue
                    self.disconnect()
                    raise ConnectionError("Old command handler read a newer message sequence!")
                response = response
                break
        return response

    def pack_command(self, *args):
        return "%s%s" % (" ".join(a for a in args if a), self.delimiter)

    @with_retry
    def execute_command(self, command_name, *args):
        self.cmd_id += 1
        command = self.pack_command(command_name, *args)
        self.send(command)
        return self.read()

    @property
    def address(self):
        if self.client_socket:
            return self.client_socket.getsockname()
        return ('', '')


class ServerPool(object):
    """
    Creates a server pool.

    :param: servers: server or list of servers.
    :param: max_pool_size: size of the pool.
    :param: blacklist_time: when a connection to a server fails, put the
            server in a blacklist for this long.
    :param: max_retries: number of times a command call will be tried
            (with different connections from the pool).
    :param: wait_for_connection: how long will it wait for an available
            connection from the pool.
    :param: max_age: for how long a connection will remain connected
            with the server.
    :param: max_connect_retries: number of times a connection will retry
            before giving up and give control for trying a command with
            some other connection in the pool.
    :param: reconnect_delay: how long will a connection wait before
            retrying to reconnect.
    :param: socket_timeout: socket timeout for operations.

    """
    connection_class = Connection

    def __init__(self, servers, max_pool_size=35, blacklist_time=60,
                 wait_for_connection=None, max_age=60,
                 max_connect_retries=2, reconnect_delay=0.1,
                 socket_timeout=4, socket_class=socket.socket, sleep=time.sleep,
                 encoding='utf-8', encoding_errors='strict'):
        self.socket_class = socket_class
        self.sleep = sleep
        self.max_connect_retries = max_connect_retries
        self.reconnect_delay = reconnect_delay
        self.socket_timeout = socket_timeout
        self.blacklist_time = blacklist_time
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self._blacklist = {}
        self._pick_index = 0
        self._pool = ConnectionPool(
            self,
            maxsize=max_pool_size,
            wait_for_connection=wait_for_connection,
            max_age=max_age,
        )
        if isinstance(servers, basestring):
            self._servers = set(s.strip() for s in SPLIT_RE.split(servers) if s.strip())
        else:
            self._servers = set(servers)

    def _pick_server(self):
        # Update the blacklist
        for server, age in self._blacklist.items():
            if time.time() - age > self.blacklist_time:
                del self._blacklist[server]

        # Build the list of available servers
        choices = list(self._servers ^ set(self._blacklist))

        if not choices:
            return None

        if self._pick_index >= len(choices):
            self._pick_index = 0

        choice = choices[self._pick_index]
        self._pick_index += 1
        return choice

    def _blacklist_server(self, server):
        self._blacklist[server] = time.time()

    def factory(self):
        server = self._pick_server()
        exc_info = None

        while server is not None:
            host, _, port = server.partition(':')
            connection = self.connection_class(
                host=host,
                port=int(port or 8890),
                max_connect_retries=self.max_connect_retries,
                reconnect_delay=self.reconnect_delay,
                socket_timeout=self.socket_timeout,
                encoding=self.encoding,
                encoding_errors=self.encoding_errors,
                socket_class=self.socket_class,
                sleep=self.sleep,
            )
            try:
                connection.connect()
                return connection
            except (socket.timeout, socket.error, ConnectionError) as exc:
                if isinstance(exc, socket.error):
                    if exc.errno != ECONNREFUSED:
                        raise  # Unmanaged case yet.
                # Blacklist this server and try again...
                self._blacklist_server(server)
                server = self._pick_server()
                exc_info = sys.exc_info()

        if exc_info is not None:
            raise exc_info[0], exc_info[1], exc_info[2]
        else:
            raise socket.timeout("No server left in the pool")

    def checkout(self):
        return self._pool.checkout()

    @contextlib.contextmanager
    def connection(self):
        """Context-manager to obtain a Client object from the pool."""
        connection = self.checkout()
        try:
            yield connection
        finally:
            connection.checkin()

    def __call__(self, callback, *args, **kwargs):
        with self.connection as connection:
            return callback(connection, *args, **kwargs)

    def call(self, name, *args, **kwargs):
        def callback(connection):
            return getattr(connection, name)(*args, **kwargs)
        return self(callback)

    def __getattr__(self, attr):
        func = getattr(self.connection_class, attr)
        command = func.command
        if not command:
            raise AttributeError
        return lambda *args, **kwargs: self.call(attr, *args, **kwargs)
