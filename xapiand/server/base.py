from __future__ import unicode_literals, absolute_import

import time
import threading
import logging

from functools import wraps

import gevent
from gevent import socket
from gevent.server import StreamServer
from gevent.threadpool import ThreadPool

from ..utils import format_time


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
            "%s from %s %s%s ~%s (%0.3f cps)",
            message % self.cmd_id,
            self.origin,
            self.cmd,
            " -> %s" % results if results is not None else "",
            format_time(cmd_duration),
            AliveCommand.cmds_count / AliveCommand.cmds_duration,
        )
        if now - AliveCommand.cmds_start > 2 or AliveCommand.cmds_count >= 10000:
            AliveCommand.cmds_start = now
            AliveCommand.cmds_duration = 0
            AliveCommand.cmds_count = 0

    def cancelled(self):
        self.executed(message="Command %d cancelled", logger=self.log.warning)

    def error(self, e):
        self.executed(e, message="Command %d ERROR", logger=self.log.error)


def command(threaded=False, **kwargs):
    def _command(func):
        func.command = func.__name__
        func.threaded = threaded
        for attr, value in kwargs.items():
            setattr(func, attr, value)
        if threaded:
            @wraps(func)
            def wrapped(self, command, _sock, *args, **kwargs):
                client_socket = socket.socket(_sock=_sock)  # Create a gevent socket from the raw socket
                self.client_socket = client_socket
                self.socket_file = client_socket.makefile()
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
        self.server = server
        self.address = address
        self.local = threading.local()
        self.client_socket = client_socket
        self.socket_file = client_socket.makefile()
        self.closed = False
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self.cmd_id = 0

    def close(self):
        self.closed = True

    @property
    def client_socket(self):
        return self.local.client_socket

    @client_socket.setter
    def client_socket(self, value):
        self.local.client_socket = value

    @property
    def socket_file(self):
        return self.local.socket_file

    @socket_file.setter
    def socket_file(self, value):
        self.local.socket_file = value

    def readline(self):
        try:
            return self.socket_file.readline()
        except socket.error:
            pass

    def handle(self):
        line = self.readline()
        while line and not self.closed:
            try:
                self.lineReceived(line)
            except QuitCommand:
                break
            line = self.readline()

    def dispatch(self, func, line, command):
        if func.threaded:
            self.server.pool.spawn(func, command, self.client_socket._sock, line, command)
        else:
            try:
                command.executed(func(line))
            except (IOError, RuntimeError, socket.error) as e:
                command.error(e)
            except DeadException:
                command.cancelled()

    def connectionMade(self):
        pass

    def connectionLost(self):
        pass

    def sendLine(self, line):
        line += self.delimiter
        if line[0] not in ("#", " "):
            line = "%s. %s" % (self.cmd_id, line)
        self.socket_file.write(line.encode(self.encoding, self.encoding_errors))
        self.socket_file.flush()

    def lineReceived(self, line):
        pass


class CommandServer(StreamServer):
    receiver_class = ClientReceiver
    pool_size = 10

    def __init__(self, *args, **kwargs):
        log = kwargs.pop('log', logging)
        super(CommandServer, self).__init__(*args, **kwargs)
        self.log = log
        self.pool = ThreadPool(self.pool_size)
        self.clients = set()

    def buildClient(self, client_socket, address):
        return self.receiver_class(self, client_socket, address, log=self.log)

    def handle(self, client_socket, address):
        client = self.buildClient(client_socket, address)

        self.clients.add(client)
        client.connectionMade()
        try:
            client.handle()
        finally:
            self.clients.remove(client)
            client.connectionLost()

    def close(self):
        if self.closed:
            self.log.debug("Closing clients...")
            for client in self.clients:
                client.client_socket._sock.close()
        else:
            self.log.debug("Closing listener socket. Waiting for clients...")
            super(CommandServer, self).close()
            for client in self.clients:
                client.close()


class CommandReceiver(ClientReceiver):
    welcome = "# Welcome to the server! Type quit to exit."

    def connectionMade(self):
        self.log.info("New connection from %s:%d (%d open connections)" % (self.address[0], self.address[1], len(self.server.clients)))
        if self.welcome:
            self.sendLine(self.welcome)

    def connectionLost(self):
        self.log.info("Lost connection from %s:%d (%d open connections)" % (self.address[0], self.address[1], len(self.server.clients)))

    def lineReceived(self, line):
        line = line.decode(self.encoding, self.encoding_errors)
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


class PortForwarder(StreamServer):
    def __init__(self, *args, **kwargs):
        log = kwargs.pop('log', logging)
        super(PortForwarder, self).__init__(*args, **kwargs)
        self.log = log
        self.sockets = set()

    def forward(self, source, dest):
        self.sockets.add(source)
        self.sockets.add(dest)

        source_address = "%s:%s" % source.getpeername()[:2]
        dest_address = "%s:%s" % dest.getpeername()[:2]
        try:
            while True:
                try:
                    data = source.recv(4096)
                except socket.error:
                    data = None
                if not data:
                    break
                self.log.debug("%s->%s: %r", source_address, dest_address, data)
                dest.sendall(data)
        finally:
            self.sockets.discard(source)
            self.sockets.discard(dest)
            source.close()
            dest.close()

    def create_connection(self):
        # self.server_address = (None, None)
        # return create_connection(...)
        raise NotImplementedError

    def handle(self, client_socket, address):
        self.log.info("New connection from %s:%d (%d open sockets)" % (address[0], address[1], len(self.sockets)))
        try:
            server_socket = self.create_connection()
        except IOError as ex:
            self.log.error("%s:%s failed to connect to %s:%s: %s", address[0], address[1], self.server_address[0], self.server_address[1], ex)
            return

        gevent.spawn(self.forward, client_socket, server_socket)
        gevent.spawn(self.forward, server_socket, client_socket)
        # XXX only one spawn() is needed

    def close(self):
        if self.closed:
            self.log.debug("Closing forward sockets...")
            for sock in self.sockets:
                sock._sock.close()
        else:
            self.log.debug("Closing listener socket. Waiting for forwarded clients...")
            super(PortForwarder, self).close()
