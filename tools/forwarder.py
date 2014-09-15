"""Port forwarder with graceful exit.

Run the example as

  python portforwarder.py :8080 gevent.org:80

Then direct your browser to http://localhost:8080 or do "telnet localhost 8080".

When the portforwarder receives TERM or INT signal (type Ctrl-C),
it closes the listening socket and waits for all existing
connections to finish. The existing connections will remain unaffected.
The program will exit once the last connection has been closed.
"""
import sys
import time
import signal
import socket

import gevent
from gevent.server import StreamServer
from gevent.socket import create_connection
from gevent.baseserver import parse_address

import logging
log = logging.getLogger(__name__)
log.setLevel('DEBUG')


class ColoredStreamHandler(logging.StreamHandler):
    """
    Colored logging.

    http://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output

    """
    def emit(self, record):
        levelno = record.levelno
        if(levelno >= 50):
            color = "\x1b[1;37;41m"  # red
        elif(levelno >= 40):
            color = "\x1b[1;31m"  # red
        elif(levelno >= 30):
            color = "\x1b[0;33m"  # yellow
        elif(levelno >= 20):
            color = "\x1b[0;36m"  # cyan
        elif(levelno >= 10):
            color = "\x1b[1;30m"  # darkgrey
        else:
            color = "\x1b[0m"  # normal
        record.msg = u"%s%s\x1b[0m" % (color, record.msg)  # normal
        super(ColoredStreamHandler, self).emit(record)
log.addHandler(ColoredStreamHandler(sys.stderr))


class PortForwarder(StreamServer):
    def __init__(self, *args, **kwargs):
        log = kwargs.pop('log', logging)
        super(PortForwarder, self).__init__(*args, **kwargs)
        self.log = log
        self.sockets = set()

    def filter(self, source_address, dest_address, reply, data):
        self.log.debug("%s -> %s: %r", source_address, dest_address, data)
        return data

    def forward(self, source, dest, reply):
        self.sockets.add(source)
        self.sockets.add(dest)

        source_address = "%s:%s" % source.getpeername()[:2]
        dest_address = "%s:%s" % dest.getpeername()[:2]
        self.log.info("%s -> %s: OPENED!", source_address, dest_address)
        try:
            while True:
                try:
                    data = source.recv(4096)
                except socket.error as e:
                    self.log.error("Connection unexpectedly ended: %s", e)
                    break
                if not data:
                    self.log.error("Connection closed by the other end.")
                    break
                data = self.filter(source_address, dest_address, reply, data)
                if data is not None:
                    dest.sendall(data)
        finally:
            self.sockets.discard(source)
            self.sockets.discard(dest)
            source.close()
            dest.close()
            if self.buffer:
                self.log.debug("Data was left on the buffer: %s", self.buffer)
            self.log.info("%s -> %s: CLOSED!", source_address, dest_address)

    def create_connection(self):
        # return create_connection(...)
        raise NotImplementedError

    def handle(self, client_socket, address):
        self.log.info("New connection from %s:%d (%d open sockets)" % (address[0], address[1], len(self.sockets)))
        try:
            server_socket = self.create_connection()
        except IOError as ex:
            self.log.error("%s:%s failed to connect to %s:%s: %s", address[0], address[1], self.server_socket[0], self.server_socket[1], ex)
            return

        gevent.spawn(self.forward, client_socket, server_socket, False)
        gevent.spawn(self.forward, server_socket, client_socket, True)
        # XXX only one spawn() is needed

    def close(self):
        if self.closed:
            self.log.debug("Closing forward sockets...")
            for sock in self.sockets:
                sock._sock.close()
        else:
            self.log.debug("Closing listener socket. Waiting for forwarded clients...")
            super(PortForwarder, self).close()

################################################################################

MESSAGE_TYPE = {
    35: [
        'MSG_ALLTERMS',  # All Terms
        'MSG_COLLFREQ',  # Get Collection Frequency
        'MSG_DOCUMENT',  # Get Document
        'MSG_TERMEXISTS',  # Term Exists?
        'MSG_TERMFREQ',  # Get Term Frequency
        'MSG_VALUESTATS',  # Get value statistics
        'MSG_KEEPALIVE',  # Keep-alive
        'MSG_DOCLENGTH',  # Get Doc Length
        'MSG_QUERY',  # Run Query
        'MSG_TERMLIST',  # Get TermList
        'MSG_POSITIONLIST',  # Get PositionList
        'MSG_POSTLIST',  # Get PostList
        'MSG_REOPEN',  # Reopen
        'MSG_UPDATE',  # Get Updated DocCount and AvLength
        'MSG_ADDDOCUMENT',  # Add Document
        'MSG_CANCEL',  # Cancel
        'MSG_DELETEDOCUMENTTERM',  # Delete Document by term
        'MSG_COMMIT',  # Commit
        'MSG_REPLACEDOCUMENT',  # Replace Document
        'MSG_REPLACEDOCUMENTTERM',  # Replace Document by term
        'MSG_DELETEDOCUMENT',  # Delete Document
        'MSG_WRITEACCESS',  # Upgrade to WritableDatabase
        'MSG_GETMETADATA',  # Get metadata
        'MSG_SETMETADATA',  # Set metadata
        'MSG_ADDSPELLING',  # Add a spelling
        'MSG_REMOVESPELLING',  # Remove a spelling
        'MSG_GETMSET',  # Get MSet
        'MSG_SHUTDOWN',  # Shutdown
        'MSG_METADATAKEYLIST',  # Iterator for metadata keys
        'MSG_MAX'
    ],
    38: [
        'MSG_ALLTERMS',  # All Terms
        'MSG_COLLFREQ',  # Get Collection Frequency
        'MSG_DOCUMENT',  # Get Document
        'MSG_TERMEXISTS',  # Term Exists?
        'MSG_TERMFREQ',  # Get Term Frequency
        'MSG_VALUESTATS',  # Get value statistics
        'MSG_KEEPALIVE',  # Keep-alive
        'MSG_DOCLENGTH',  # Get Doc Length
        'MSG_QUERY',  # Run Query
        'MSG_TERMLIST',  # Get TermList
        'MSG_POSITIONLIST',  # Get PositionList
        'MSG_POSTLIST',  # Get PostList
        'MSG_REOPEN',  # Reopen
        'MSG_UPDATE',  # Get Updated DocCount and AvLength
        'MSG_ADDDOCUMENT',  # Add Document
        'MSG_CANCEL',  # Cancel
        'MSG_DELETEDOCUMENTTERM',  # Delete Document by term
        'MSG_COMMIT',  # Commit
        'MSG_REPLACEDOCUMENT',  # Replace Document
        'MSG_REPLACEDOCUMENTTERM',  # Replace Document by term
        'MSG_DELETEDOCUMENT',  # Delete Document
        'MSG_WRITEACCESS',  # Upgrade to WritableDatabase
        'MSG_GETMETADATA',  # Get metadata
        'MSG_SETMETADATA',  # Set metadata
        'MSG_ADDSPELLING',  # Add a spelling
        'MSG_REMOVESPELLING',  # Remove a spelling
        'MSG_GETMSET',  # Get MSet
        'MSG_SHUTDOWN',  # Shutdown
        'MSG_METADATAKEYLIST',  # Iterator for metadata keys
        'MSG_FREQS',  # Get termfreq and collfreq
        'MSG_UNIQUETERMS',  # Get number of unique terms in doc
        'MSG_MAX'
    ],
}

REPLY_TYPE = {
    35: [
        'REPLY_GREETING',  # Greeting
        'REPLY_EXCEPTION',  # Exception
        'REPLY_DONE',  # Done sending list
        'REPLY_ALLTERMS',  # All Terms
        'REPLY_COLLFREQ',  # Get Collection Frequency
        'REPLY_DOCDATA',  # Get Document
        'REPLY_TERMDOESNTEXIST',  # Term Doesn't Exist
        'REPLY_TERMEXISTS',  # Term Exists
        'REPLY_TERMFREQ',  # Get Term Frequency
        'REPLY_VALUESTATS',  # Value statistics
        'REPLY_DOCLENGTH',  # Get Doc Length
        'REPLY_STATS',  # Stats
        'REPLY_TERMLIST',  # Get Termlist
        'REPLY_POSITIONLIST',  # Get PositionList
        'REPLY_POSTLISTSTART',  # Start of a postlist
        'REPLY_POSTLISTITEM',  # Item in body of a postlist
        'REPLY_UPDATE',  # Get Updated DocCount and AvLength
        'REPLY_VALUE',  # Document Value
        'REPLY_ADDDOCUMENT',  # Add Document
        'REPLY_RESULTS',  # Results (MSet)
        'REPLY_METADATA',  # Metadata
        'REPLY_METADATAKEYLIST',  # Iterator for metadata keys
        'REPLY_MAX'
    ],
    38: [
        'REPLY_UPDATE',  # Updated database stats
        'REPLY_EXCEPTION',  # Exception
        'REPLY_DONE',  # Done sending list
        'REPLY_ALLTERMS',  # All Terms
        'REPLY_COLLFREQ',  # Get Collection Frequency
        'REPLY_DOCDATA',  # Get Document
        'REPLY_TERMDOESNTEXIST',  # Term Doesn't Exist
        'REPLY_TERMEXISTS',  # Term Exists
        'REPLY_TERMFREQ',  # Get Term Frequency
        'REPLY_VALUESTATS',  # Value statistics
        'REPLY_DOCLENGTH',  # Get Doc Length
        'REPLY_STATS',  # Stats
        'REPLY_TERMLIST',  # Get Termlist
        'REPLY_POSITIONLIST',  # Get PositionList
        'REPLY_POSTLISTSTART',  # Start of a postlist
        'REPLY_POSTLISTITEM',  # Item in body of a postlist
        'REPLY_VALUE',  # Document Value
        'REPLY_ADDDOCUMENT',  # Add Document
        'REPLY_RESULTS',  # Results (MSet)
        'REPLY_METADATA',  # Metadata
        'REPLY_METADATAKEYLIST',  # Iterator for metadata keys
        'REPLY_FREQS',  # Get termfreq and collfreq
        'REPLY_UNIQUETERMS',  # Get number of unique terms in doc
        'REPLY_MAX'
    ],
}


class XapiandForwarder(PortForwarder):
    def __init__(self, listener, server, *args, **kwargs):
        super(XapiandForwarder, self).__init__(listener, *args, **kwargs)
        self.set_server(server)
        address = self.address[0] or '0.0.0.0'
        port = self.address[1]
        self.log.warning("Xapiand Forwarder Listening to %s:%s", address, port)
        self.buffer = ''
        self.time = time.time()

    def set_server(self, server):
        if hasattr(server, 'accept'):
            if hasattr(server, 'do_handshake'):
                raise TypeError('Expected a regular socket, not SSLSocket: %r' % (server, ))
            self.server_family = server.family
            self.server_address = server.getsockname()
        else:
            self.server_family, self.server_address = parse_address(server)

    def create_connection(self):
        return create_connection(self.server_address)

    def filter(self, source_address, dest_address, reply, data):
        self.log.debug("%s -> %s: %r", source_address, dest_address, data)
        now = time.time()
        if now - self.time > 0.1:
            self.log.debug('')
            self.log.debug('=' * 88)
        self.time = now
        try:
            _data = self.buffer + data
            self.buffer = ''
            while _data:
                cmd = ord(_data[0])
                sz = ord(_data[1])
                obj = _data[2:sz + 2]
                if len(obj) != sz:
                    self.buffer = _data
                    break
                _data = _data[2 + sz:]
                decoded = ''
                if reply and cmd == 0:
                    # REPLY_GREETING <protocol major version> <protocol minor version> I<db doc count> I<last docid> B<has positions?> I<db total length> <UUID>
                    self.major_version = ord(obj[0])
                    self.minor_version = ord(obj[1])
                    uuid = obj[8:]
                    decoded = " = v%s.%s (%s)" % (self.major_version, self.minor_version, uuid)
                try:
                    if reply:
                        msg = REPLY_TYPE[self.major_version]
                    else:
                        msg = MESSAGE_TYPE[self.major_version]
                except KeyError:
                    self.log.error("    Unsupported version: %s (%s)", self.major_version, chr(self.major_version))
                    continue
                try:
                    msg = msg[cmd]
                except IndexError:
                    self.log.error("    Unknown %s command: %s", 'REPLY_TYPE' if reply else 'MESSAGE_TYPE', cmd)
                    continue
                self.log.info("    %s[%d]: %r%s", msg, sz, obj, decoded)
        except Exception:
            import traceback; traceback.print_exc();
        return data


def main():
    args = sys.argv[1:]
    if len(args) != 2:
        sys.exit('Usage: %s listener server' % __file__)
    listener = args[0]
    server = args[1]
    server = XapiandForwarder(listener, server, log=log)
    gevent.signal(signal.SIGTERM, server.close)
    gevent.signal(signal.SIGINT, server.close)
    server.start()
    gevent.wait()


if __name__ == '__main__':
    main()
