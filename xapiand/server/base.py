from __future__ import unicode_literals, absolute_import, print_function

import time
import logging
import weakref
import threading
from hashlib import md5

from functools import wraps

from gevent import socket
from gevent.server import StreamServer
from gevent.threadpool import ThreadPool
from ..utils import format_time, sendall, readline


class QuitCommand(Exception):
    pass


class DeadException(Exception):
    def __init__(self, command):
        self.command = command


class AliveCommand(object):
    """
    Raises DeadException if the object's cmd_id id is not the same
    as it was when the object was created.

    """
    cmds_duration = 0
    cmds_start = 0
    cmds_count = 0

    def __init__(self, parent, cmd, origin):
        parent.cmd_id = getattr(parent, 'cmd_id', 0) + 1
        self.parent = parent
        self.cmd_id = parent.cmd_id
        self.cmd = cmd
        self.origin = origin
        self.log = parent.log
        self.start = time.time()

    def __nonzero__(self):
        if self.cmd_id == self.parent.cmd_id:
            return False
        raise DeadException(self)

    def executed(self, results, message="Executed command %d", logger=None):
        if logger is None:
            logger = self.log.debug
        now = time.time()
        cmd_duration = now - self.start
        AliveCommand.cmds_duration += cmd_duration
        AliveCommand.cmds_count += 1
        logger(
            "%s %s%s by %s ~%s (%0.3f cps)",
            message % self.cmd_id,
            self.cmd,
            " -> %s" % results if results is not None else "",
            self.origin,
            format_time(cmd_duration),
            AliveCommand.cmds_count / AliveCommand.cmds_duration,
        )
        if now - AliveCommand.cmds_start > 2 or AliveCommand.cmds_count >= 10000:
            AliveCommand.cmds_start = now
            AliveCommand.cmds_duration = 0
            AliveCommand.cmds_count = 0

    def cancelled(self):
        self.executed(None, message="Command %d cancelled", logger=self.log.warning)

    def error(self, e):
        self.executed(e, message="Command %d ERROR", logger=self.log.error)


def command(threaded=False, **kwargs):
    def _command(func):
        func.command = func.__name__
        func.threaded = threaded
        for attr, value in kwargs.items():
            setattr(func, attr, value)
        if func.threaded:
            @wraps(func)
            def wrapped(self, command, client_socket, *args, **kwargs):
                current_thread = threading.current_thread()
                tid = current_thread.name.rsplit('-', 1)[-1]
                current_thread.name = '%s-%s-%s' % (self.client_id[:14], command.cmd, tid)

                # Create a gevent socket for this thread from the other tread's socket
                # (using the raw underlying socket, '_sock'):
                client_socket = socket.socket(_sock=client_socket._sock)

                self.client_socket = client_socket
                try:
                    command.executed(func(self, *args, **kwargs))
                except (IOError, RuntimeError, socket.error) as e:
                    command.error(e)
                except DeadException:
                    command.cancelled()
            return wrapped
        else:
            return func
    if callable(threaded):
        func, threaded = threaded, False
        return _command(func)
    return _command


class ClientReceiver(object):
    delimiter = b'\r\n'

    def __init__(self, server, client_socket, address, log=logging,
                 encoding='utf-8', encoding_errors='strict'):
        self.log = log
        self._server = weakref.ref(server)
        self.address = address
        self.local = threading.local()
        self.client_socket = client_socket
        self.closed = False
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.cmd_id = 0
        self.activity = time.time()

        self.client_id = ("Client-%s" % md5('%s:%s' % (address[0], address[1])).hexdigest())
        current_thread = threading.current_thread()
        tid = current_thread.name.rsplit('-', 1)[-1]
        current_thread.name = '%s-%s' % (self.client_id[:14], tid)

    @property
    def server(self):
        return self._server()

    def close(self):
        self.closed = True

    @property
    def client_socket(self):
        return self.local.client_socket

    @client_socket.setter
    def client_socket(self, value):
        self.local.client_socket = value

    def handle(self):
        for line in readline(self.client_socket, encoding=self.encoding, encoding_errors=self.encoding_errors):
            if not line or self.closed:
                break
            try:
                self.lineReceived(line)
            except QuitCommand:
                break

    def dispatch(self, func, line, command):
        if func.threaded:
            commands_pool = self.server.pool
            pool_size = self.server.pool_size
            pool_size_warning = self.server.pool_size_warning
            commands_pool.spawn(func, command, self.client_socket, line, command)
            pool_used = len(commands_pool)
            if not (pool_size_warning - pool_used) % 10:
                self.log.warning("Commands pool is close to be full (%s/%s)", pool_used, pool_size)
            elif pool_used == pool_size:
                self.log.error("Commands poll is full! (%s/%s)", pool_used, pool_size)
        else:
            try:
                command.executed(func(line))
            except (IOError, RuntimeError, socket.error) as e:
                command.error(e)
            except DeadException:
                command.cancelled()

    def connectionMade(self, client):
        pass

    def connectionLost(self, client):
        pass

    def sendLine(self, line):
        line += self.delimiter
        if line[0] not in ("#", " "):
            line = "%s. %s" % (self.cmd_id, line)
        sendall(self.client_socket, line, encoding=self.encoding, encoding_errors=self.encoding_errors)

    def lineReceived(self, line):
        self.activity = time.time()


class CommandServer(StreamServer):
    receiver_class = ClientReceiver
    pool_size = 10

    def __init__(self, *args, **kwargs):
        self.log = kwargs.pop('log', logging)
        self.pool_size = kwargs.pop('pool_size', self.pool_size)
        super(CommandServer, self).__init__(*args, **kwargs)
        self.pool_size_warning = int(self.pool_size / 3.0 * 2.0)
        self.pool = ThreadPool(self.pool_size)
        self.clients = set()

    def build_client(self, client_socket, address):
        return self.receiver_class(self, client_socket, address, log=self.log)

    def handle(self, client_socket, address):
        client = self.build_client(client_socket, address)

        self.clients.add(client)
        client.connectionMade(client)
        try:
            client.handle()
        finally:
            self.clients.discard(client)
            client.connectionLost(client)

    def close(self, max_age=None):
        if self.closed:
            if max_age is None:
                self.log.error("Forcing server shutdown (%s clients)...", len(self.clients))
        else:
            if max_age is None:
                max_age = 10
            self.log.warning("Hitting Ctrl+C again will terminate all running tasks!")
            super(CommandServer, self).close()

        now = time.time()
        clean = []
        for client in self.clients:
            if max_age is None or client._weak or now - client.activity > max_age:
                try:
                    client.client_socket._sock.close()
                except AttributeError:
                    pass
                clean.append(client)

        for client in clean:
            self.clients.discard(client)

        return not bool(self.clients)


class CommandReceiver(ClientReceiver):
    welcome = "# Welcome to the server! Type quit to exit."

    def __init__(self, *args, **kwargs):
        self._weak = False
        super(CommandReceiver, self).__init__(*args, **kwargs)

    def connectionMade(self, client):
        self.log.info("New connection from %s: %s:%d (%d open connections)" % (client.client_id, self.address[0], self.address[1], len(self.server.clients)))
        if self.welcome:
            self.sendLine(self.welcome)

    def connectionLost(self, client):
        self.log.info("Lost connection (%d open connections)" % len(self.server.clients))

    def lineReceived(self, line):
        super(CommandReceiver, self).lineReceived(line)
        cmd, _, line = line.partition(' ')
        cmd = cmd.strip().lower()
        line = line.strip()
        if not cmd:
            return
        try:
            func = getattr(self, cmd)
            if not func.command:
                raise AttributeError
        except AttributeError:
            self.sendLine(">> ERR: [404] Unknown command: %s" % cmd.upper())
        else:
            command = AliveCommand(self, cmd=cmd.upper(), origin="%s:%d" % (self.address[0], self.address[1]))
            self.dispatch(func, line, command)

    @command
    def quit(self, line):
        """
        Closes connection with server.

        Usage: QUIT

        """
        self.sendLine(">> BYE!")
        raise QuitCommand
    exit = quit

    @command(internal=True)
    def weak(self, line=''):
        """
        Makes the connection weak (closes when the server needs to)

        """
        self._weak = True
        self.sendLine(">> OK")

    def _help(self, func, cmd):
        # Figure out indentation for docstring:
        doc = func.__doc__ or "No docs for %s." % cmd
        doc = doc.strip('\n').split('\n')
        indent = 0
        for l in doc:
            indent = len(l) - len(l.lstrip())
            if indent:
                break
        # Remove ending empty lines:
        doc = '\n'.join(l[indent:].rstrip() for l in doc).strip('\n').split('\n')
        return doc

    @command
    def help(self, line):
        """
        Returns help for any and all available commands.

        Usage: HELP [command]

        """
        cmd = line.strip().lower() or 'help'
        try:
            func = getattr(self, cmd)
            if not func.command or getattr(func, 'internal', False):
                raise AttributeError
        except AttributeError:
            self.sendLine(">> ERR: [404] Unknown command: %s" % cmd)
        else:
            doc = self._help(func, cmd)
            if cmd == 'help':
                docs = {}
                length = 0
                for _cmd in dir(self):
                    _func = getattr(self, _cmd)
                    command = getattr(_func, 'command', None)
                    if command and getattr(_func, 'internal', False):
                        if command not in docs:
                            _help = self._help(_func, command)[0]
                            docs[command] = [_help, []]
                        docs[command][1].append(_cmd)
                        length = max(length, len(_cmd))
                pat = "  %%%ds," % length
                for _help, _cmds in docs.values():
                    _doc = [pat % _cmd.upper() for _cmd in _cmds]
                    _doc[-1] = "%s - %s" % (_doc[-1][:-1], _help)
                    doc.extend(_doc)
            # Indent and send results:
            doc = self.delimiter.join("    " + l for l in doc)
            self.sendLine(">> OK: %s::\n%s" % (cmd.upper(), doc))
