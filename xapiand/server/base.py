from __future__ import unicode_literals, absolute_import

import time
import threading

from functools import wraps

from gevent import socket
from gevent.server import StreamServer
from gevent.threadpool import ThreadPool


class QuitCommand(Exception):
    pass


def command(threaded=False, **kwargs):
    def _command(func):
        func.command = func.__name__
        func.threaded = threaded
        for attr, value in kwargs.items():
            setattr(func, attr, value)
        if threaded:
            @wraps(func)
            def wrapped(self, _sock, *args, **kwargs):
                client_socket = socket.socket(_sock=_sock)  # Create a gevent socket from the raw socket
                self.client_socket = client_socket
                self.socket_file = client_socket.makefile()
                return func(self, *args, **kwargs)
            return wrapped
        else:
            return func
    if callable(threaded):
        func, threaded = threaded, False
        return _command(func)
    return _command


class ClientReceiver(object):
    delimiter = '\r\n'

    def __init__(self, server, client_socket, address, log=None):
        self.log = log
        self.server = server
        self.address = address
        self.local = threading.local()
        self.client_socket = client_socket
        self.socket_file = client_socket.makefile()
        self.closed = False

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

    def dispatch(self, func, line):
        if func.threaded:
            self.server.pool.spawn(func, self.client_socket._sock, line)
        else:
            func(line)

    def connectionMade(self):
        pass

    def connectionLost(self):
        pass

    def sendLine(self, line):
        self.socket_file.write(line.encode('utf8') + self.delimiter)
        self.socket_file.flush()

    def lineReceived(self, line):
        pass


class CommandServer(StreamServer):
    receiver_class = ClientReceiver
    pool_size = 10

    def __init__(self, *args, **kwargs):
        log = kwargs.pop('log', None)
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

    def __init__(self, *args, **kwargs):
        super(CommandReceiver, self).__init__(*args, **kwargs)
        self.cmd = None
        self.cmd_duration = 0
        self.cmd_start = 0
        self.cmd_count = 0

    def connectionMade(self):
        self.log.info("New connection from %s:%d (%d open connections)" % (self.address[0], self.address[1], len(self.server.clients)))
        if self.welcome:
            self.sendLine(self.welcome)

    def connectionLost(self):
        self.log.info("Lost connection from %s:%d (%d open connections)" % (self.address[0], self.address[1], len(self.server.clients)))

    def lineReceived(self, line):
        line = line.decode('utf-8')
        cmd, _, line = line.partition(' ')
        cmd = cmd.strip().lower()
        line = line.strip()
        if not cmd:
            return
        start = time.time()
        try:
            func = getattr(self, cmd)
            if not func.command:
                raise AttributeError
        except AttributeError:
            self.sendLine(">> ERR: [404] Unknown command: %s" % cmd.upper())
        else:
            self.dispatch(func, line)
            now = time.time()
            self.cmd_duration += now - start
            self.cmd_count += 1
            self.cmd = cmd
            if now - self.cmd_start > 2 or self.cmd_count >= 10000:
                if self.cmd_count == 1:
                    self.log.debug("Executed command %s from %s:%d ~ %0.3f s", self.cmd.upper(), self.address[0], self.address[1], self.cmd_duration)
                else:
                    self.log.info("Executed %s commands from %s:%d ~ %0.3f s (%0.3f cps)", self.cmd_count, self.address[0], self.address[1], self.cmd_duration, self.cmd_count / self.cmd_duration)
                self.cmd_start = now
                self.cmd_duration = 0
                self.cmd_count = 0

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
            if not func.command:
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
                    if command:
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
