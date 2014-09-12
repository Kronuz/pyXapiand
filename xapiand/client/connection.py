from __future__ import absolute_import, unicode_literals

import sys
import time
import Queue
import socket
import contextlib
import threading

from errno import EISCONN, EINVAL, ECONNREFUSED
from functools import wraps

from ..exceptions import ConnectionError, NewConnection


# Sentinel used to mark an empty slot in the ConnectionPool queue.
# Using sys.maxint as the timestamp ensures that empty slots will always
# sort *after* live connection objects in the queue.
EMPTY_SLOT = (sys.maxint, None)


def log_command(func):
    @wraps(func)
    def wrapped(self, command_name, *args):
        start = time.time()
        try:
            return func(self, command_name, *args)
        finally:
            stop = time.time()
            self.pool.queries.setdefault(self.get_name(), []).append({
                'command_name': command_name,
                'additional_args': args,
                'time': "%.3f s" % (stop - start),
                'start': start,
                'stop': stop,
            })
    return wrapped


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
    def __init__(self, factory, maxsize=None, timeout=60,
                 wait_for_connection=None):
        self._context_tl = threading.local()
        self.factory = factory
        self.maxsize = maxsize
        self.timeout = timeout
        self.clients = Queue.PriorityQueue(maxsize)
        self.wait_for_connection = wait_for_connection
        # If there is a maxsize, prime the queue with empty slots.
        if maxsize is not None:
            for _ in xrange(maxsize):
                self.clients.put(EMPTY_SLOT)

    @contextlib.contextmanager
    def reserve(self):
        """Context-manager to obtain a Client object from the pool."""
        ts, connection = self._checkout_connection()
        try:
            yield connection
        finally:
            self._checkin_connection(ts, connection)

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
                    now = int(time.time())
                    connection = self.factory(self._context_tl)
                    return now, connection
            else:
                now = int(time.time())
                # If we got an empty slot placeholder, create a new connection.
                if connection is None:
                    try:
                        connection = self.factory(self._context_tl)
                        return now, connection
                    except Exception:
                        if self.maxsize is not None:
                            # return slot to queue
                            self.clients.put(EMPTY_SLOT)
                        raise
                # If the connection is not stale, go ahead and use it.
                if ts + self.timeout > now:
                    connection.context = self._context_tl
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
        now = int(time.time())
        if ts + self.timeout > now:
            self.clients.put((ts, connection))
        else:
            if self.maxsize is not None:
                self.clients.put(EMPTY_SLOT)


def with_retry(func):
    @wraps(func)
    def _with_retry(self, *args, **kw):
        retries = 0
        delay = self.reconnect_delay

        while retries < self.max_connect_retries:
            try:
                return func(self, *args, **kw)
            except NewConnection:
                continue
            except (IOError, RuntimeError, socket.error, ConnectionError):
                exc_info = sys.exc_info()
                time.sleep(delay)
                retries += 1
                delay *= 3      # growing the delay

        raise exc_info[0], exc_info[1], exc_info[2]
    return _with_retry


class Connection(object):
    MAX_READ_LENGTH = 1000000
    delimiter = '\r\n'

    def __init__(self, pool, host='localhost', port=8890, endpoints=None,
                 max_connect_retries=5, reconnect_delay=0.5,
                 socket_timeout=3, encoding='utf-8',
                 encoding_errors='strict'):
        self.pool = pool
        self.host = host
        self.port = port
        self.endpoints = endpoints
        self.max_connect_retries = max_connect_retries
        self.reconnect_delay = reconnect_delay
        self.socket_timeout = socket_timeout
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.client_socket = None
        self.socket_file = None
        self.cmd_id = 0

    def __del__(self):
        try:
            self.disconnect()
        except Exception:
            pass

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

    def connect(self):
        if not self.client_socket:
            try:
                sock = self._connect()
            except socket.error:
                exc_info = sys.exc_info()
                e = exc_info[1]
                raise (
                    ConnectionError,
                    ConnectionError(self._error_message(e)),
                    exc_info[2])
            self.client_socket = sock
        if not self.socket_file:
            self.socket_file = self.client_socket.makefile()
        self.cmd_id = 0
        self.on_connect()

    def _connect(self):
        "Create a TCP socket connection"
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)

        retries = 0
        delay = self.reconnect_delay

        while retries < self.max_connect_retries:
            try:
                sock.connect((self.host, self.port))
                return sock
            except socket.error as exc:
                exc_info = sys.exc_info()
                if exc.errno == EISCONN:
                    return sock   # we're good
                if exc.errno == EINVAL:
                    # we're doomed, recreate socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(self.socket_timeout)

                time.sleep(delay)
                retries += 1
                delay *= 2      # growing the delay

        raise exc_info[0], exc_info[1], exc_info[2]

    def disconnect(self):
        "Disconnects from the server"
        self.on_disconnect()
        self.cmd_id = 0
        if self.socket_file is not None:
            self.socket_file.close()
            self.socket_file = None
        if self.client_socket is not None:
            try:
                self.client_socket.shutdown(socket.SHUT_RDWR)
                self.client_socket.close()
            except socket.error:
                pass
            self.client_socket = None

    def send(self, body):
        if not self.client_socket:
            self.connect()
            raise NewConnection("New connection made!")
        # print '<<<<---', id(self), '%s:%s' % (self.address[0], self.address[1]), repr(body)
        try:
            self.socket_file.write(body)
            self.socket_file.flush()
        except socket.error:
            self.disconnect()
            exc_info = sys.exc_info()
            e = exc_info[1]
            if len(e.args) == 1:
                _errno, errmsg = 'UNKNOWN', e.args[0]
            else:
                _errno, errmsg = e.args
            raise (
                ConnectionError,
                ConnectionError("Error %s while writing to socket. %s." % (_errno, errmsg)),
                exc_info[2])
        except Exception:
            self.disconnect()
            raise

    def read(self):
        "Read the response from a previously sent command"
        cmd_id = self.cmd_id
        while True:
            try:
                response = self.socket_file.readline()
                if not response:
                    self.disconnect()
                    raise ConnectionError("No response!")
                response = response[:-2]
                # print '--->>>>', id(self), '%s:%s' % (self.address[0], self.address[1]), repr(response)
                if response:
                    if response[0] in (b"#", b" "):
                        continue
                    try:
                        _cmd_id, _, response = response.partition(b'. ')
                    except ValueError:
                        continue
                    _cmd_id = int(_cmd_id)
                    if _cmd_id != cmd_id:
                        if _cmd_id < cmd_id:
                            continue
                        self.disconnect()
                        raise ConnectionError("Old command handler read a newer message sequence!")
                    response = response.decode(self.encoding, self.encoding_errors)
                return response
            except (socket.error, socket.timeout):
                self.disconnect()
                exc_info = sys.exc_info()
                e = exc_info[1]
                raise (
                    ConnectionError,
                    ConnectionError("Error while reading from socket: %s" % (e.args,)),
                    exc_info[2])

    def pack_command(self, *args):
        return "%s%s" % (" ".join(a for a in args if a), self.delimiter)

    # @log_command
    @with_retry
    def execute_command(self, command_name, *args):
        self.cmd_id += 1
        command = self.pack_command(command_name, *args)
        self.send(command.encode(self.encoding, self.encoding_errors))
        return self.read()

    @property
    def address(self):
        if self.client_socket:
            return self.client_socket.getsockname()
        return ('', '')


class ServerPool(object):
    def __init__(self, server, max_retries=3, max_pool_size=35, socket_timeout=4, blacklist_time=60):
        self.queries = {}

        self.max_retries = max_retries
        self.max_pool_size = max_pool_size
        self.socket_timeout = socket_timeout
        self.blacklist_time = blacklist_time
        self._blacklist = {}
        self._pick_index = 0
        self._pool = ConnectionPool(
            self._client_factory,
            maxsize=self.max_pool_size,
            wait_for_connection=self.socket_timeout,
        )
        if isinstance(server, basestring):
            self._servers = server.split(';')
        else:
            self._servers = server
        for attr in dir(self.connection_class):
            func = getattr(self.connection_class, attr)
            try:
                command = func.command
            except AttributeError:
                continue
            if command:
                def func(name):
                    return lambda *args, **kwargs: self.call(name, *args, **kwargs)
                setattr(self, attr, func(attr))

    def call(self, name, *args, **kwargs):
        retries = 0

        while retries < self.max_retries:
            with self._pool.reserve() as connection:
                try:
                    func = getattr(connection, name)
                except AttributeError:
                    exc_info = sys.exc_info()
                    retries = self.max_retries
                else:
                    try:
                        return func(*args, **kwargs)
                    except (IOError, RuntimeError, socket.error, ConnectionError):
                        exc_info = sys.exc_info()
                        retries += 1

        raise exc_info[0], exc_info[1], exc_info[2]

    def _pick_server(self):
        # update the blacklist
        for server, age in self._blacklist.items():
            if time.time() - age > self.blacklist_time:
                del self._blacklist[server]

        # build the list of available servers
        choices = list(set(self._servers) ^ set(self._blacklist.keys()))

        if not choices:
            return None

        if self._pick_index >= len(choices):
            self._pick_index = 0

        choice = choices[self._pick_index]
        self._pick_index += 1
        return choice

    def _blacklist_server(self, server):
        self._blacklist[server] = time.time()

    def _client_factory(self, context):
        server = self._pick_server()
        last_error = None

        while server is not None:
            host, _, port = server.partition(':')
            connection = self.connection_class(self, host=host, port=int(port or 8890), socket_timeout=self.socket_timeout)
            connection.context = context
            try:
                connection.connect()
                return connection
            except (socket.timeout, socket.error, ConnectionError) as exc:
                if not isinstance(exc, socket.timeout):
                    if exc.errno != ECONNREFUSED:
                        # unmanaged case yet
                        raise

                # well that's embarrassing, let's blacklist this one
                # and try again
                self._blacklist_server(server)
                server = self._pick_server()
                last_error = exc

        if last_error is not None:
            raise last_error
        else:
            raise socket.timeout("No server left in the pool")
