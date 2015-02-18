from __future__ import unicode_literals, absolute_import, print_function

import os
import sys
import time
import signal
import logging
import collections
import threading

from functools import wraps

import gevent
from gevent import queue
from gevent import socket
from gevent.server import StreamServer
from gevent.threadpool import ThreadPool

from ..core import DatabasesPool
from ..utils import format_time

LOG_FORMAT = "[%(asctime)s: %(levelname)s/%(processName)s:%(threadName)s] %(message)s"

handler = logging.StreamHandler(sys.stderr)
handler.setFormatter(logging.Formatter(LOG_FORMAT))

logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


class ConnectionClosed(Exception):
    pass


class InvalidCommand(Exception):
    pass


class KeepTrying(Exception):
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
        self.start = time.time()

    def __nonzero__(self):
        if self.cmd_id == self.parent.cmd_id:
            return False
        raise DeadException(self)

    def executed(self, results, message="Executed command %d", log=None):
        if log is None:
            log = logger.debug
        now = time.time()
        cmd_duration = now - self.start
        AliveCommand.cmds_duration += cmd_duration
        AliveCommand.cmds_count += 1
        log(
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
        self.executed(None, message="Command %d cancelled", log=logger.warning)

    def error(self, e):
        self.executed(e, message="Command %d ERROR", log=logger.error)


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
                self.client_socket = socket.socket(_sock=client_socket._sock)

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


COMMIT_TIMEOUT = 1
COMMANDS_POOL_SIZE = 100

MESSAGE_TYPES = [
    'MSG_ALLTERMS',             # All Terms
    'MSG_COLLFREQ',             # Get Collection Frequency
    'MSG_DOCUMENT',             # Get Document
    'MSG_TERMEXISTS',           # Term Exists?
    'MSG_TERMFREQ',             # Get Term Frequency
    'MSG_VALUESTATS',           # Get value statistics
    'MSG_KEEPALIVE',            # Keep-alive
    'MSG_DOCLENGTH',            # Get Doc Length
    'MSG_QUERY',                # Run Query
    'MSG_TERMLIST',             # Get TermList
    'MSG_POSITIONLIST',         # Get PositionList
    'MSG_POSTLIST',             # Get PostList
    'MSG_REOPEN',               # Reopen
    'MSG_UPDATE',               # Get Updated DocCount and AvLength
    'MSG_ADDDOCUMENT',          # Add Document
    'MSG_CANCEL',               # Cancel
    'MSG_DELETEDOCUMENTTERM',   # Delete Document by term
    'MSG_COMMIT',               # Commit
    'MSG_REPLACEDOCUMENT',      # Replace Document
    'MSG_REPLACEDOCUMENTTERM',  # Replace Document by term
    'MSG_DELETEDOCUMENT',       # Delete Document
    'MSG_WRITEACCESS',          # Upgrade to WritableDatabase
    'MSG_GETMETADATA',          # Get metadata
    'MSG_SETMETADATA',          # Set metadata
    'MSG_ADDSPELLING',          # Add a spelling
    'MSG_REMOVESPELLING',       # Remove a spelling
    'MSG_GETMSET',              # Get MSet
    'MSG_SHUTDOWN',             # Shutdown
    'MSG_METADATAKEYLIST',      # Iterator for metadata keys
    'MSG_FREQS',                # Get termfreq and collfreq
    'MSG_UNIQUETERMS',          # Get number of unique terms in doc
    'MSG_SELECT',               # Select current database
]
MessageType = collections.namedtuple('MessageType', MESSAGE_TYPES)
MESSAGE = MessageType(**dict((attr, i) for i, attr in enumerate(MESSAGE_TYPES)))

REPLY_TYPES = [
    'REPLY_UPDATE',             # Updated database stats
    'REPLY_EXCEPTION',          # Exception
    'REPLY_DONE',               # Done sending list
    'REPLY_ALLTERMS',           # All Terms
    'REPLY_COLLFREQ',           # Get Collection Frequency
    'REPLY_DOCDATA',            # Get Document
    'REPLY_TERMDOESNTEXIST',    # Term Doesn't Exist
    'REPLY_TERMEXISTS',         # Term Exists
    'REPLY_TERMFREQ',           # Get Term Frequency
    'REPLY_VALUESTATS',         # Value statistics
    'REPLY_DOCLENGTH',          # Get Doc Length
    'REPLY_STATS',              # Stats
    'REPLY_TERMLIST',           # Get Termlist
    'REPLY_POSITIONLIST',       # Get PositionList
    'REPLY_POSTLISTSTART',      # Start of a postlist
    'REPLY_POSTLISTITEM',       # Item in body of a postlist
    'REPLY_VALUE',              # Document Value
    'REPLY_ADDDOCUMENT',        # Add Document
    'REPLY_RESULTS',            # Results (MSet)
    'REPLY_METADATA',           # Metadata
    'REPLY_METADATAKEYLIST',    # Iterator for metadata keys
    'REPLY_FREQS',              # Get termfreq and collfreq
    'REPLY_UNIQUETERMS',        # Get number of unique terms in doc
]
ReplyType = collections.namedtuple('ReplyType', REPLY_TYPES)
REPLY = ReplyType(**dict((attr, i) for i, attr in enumerate(REPLY_TYPES)))


def encode_length(decoded):
    if decoded < 255:
        encoded = chr(decoded)
    else:
        encoded = b'\xff'
        decoded -= 255
        while True:
            b = decoded & 0x7f
            decoded >>= 7
            if decoded:
                encoded += chr(b)
            else:
                encoded += chr(b | 0x80)
                break
    return encoded


def decode_length(encoded):
    decoded = encoded[0]
    if decoded == b'\xff':
        encoded = encoded[1:]
        decoded = 0
        shift = 0
        size = 1
        for ch in encoded:
            ch = ord(ch)
            decoded |= (ch & 0x7f) << shift
            shift += 7
            size += 1
            if ch & 0x80:
                break
        else:
            raise ValueError()
        decoded += 255
    else:
        decoded = ord(decoded)
        size = 1
    return decoded, size


class ClientReceiver(object):
    def __init__(self, server, client_socket, address):
        self.weak_client = False

        self.closed = False
        self.server = server
        self.client_socket = client_socket
        self.address = address
        self.cmd_id = 0
        self.activity = time.time()
        self.buf = b''

        self.client_id = "Client-%s" % (hash((address[0], address[1])) & 0xffffff)
        current_thread = threading.current_thread()
        tid = current_thread.name.rsplit('-', 1)[-1]
        current_thread.name = '%s-%s' % (self.client_id[:14], tid)

        self.message_type = MessageType(**dict((attr, getattr(self, attr.lower())) for attr in MESSAGE_TYPES))

        self.endpoints = []

    def send(self, msg):
        # logger.debug(">>> %s", repr(msg))
        return self.client_socket.sendall(msg)

    def read(self, size):
        msg = self.client_socket.recv(size)
        # logger.debug("<<< %s", repr(msg))
        return msg

    def connectionMade(self, client):
        logger.info("New connection from %s: %s:%d (%d open connections)" % (client.client_id, self.address[0], self.address[1], len(self.server.clients)))
        self.reply_update()

    def connectionLost(self, client):
        logger.info("Lost connection (%d open connections)" % len(self.server.clients))

    def get_message(self, required_type=None):
        tmp = self.read(1024)
        if not tmp:
            ConnectionClosed
        self.buf += tmp
        try:
            func = self.message_type[ord(self.buf[0])]
        except (TypeError, IndexError):
            raise InvalidCommand
        try:
            length, stride = decode_length(self.buf[1:])
        except ValueError:
            raise KeepTrying
        message = self.buf[1 + stride:1 + stride + length]
        if len(message) != length:
            raise KeepTrying
        self.buf = self.buf[1 + stride + length:]
        self.activity = time.time()
        return func, message

    def handle(self):
        try:
            while not self.closed:
                try:
                    func, message = self.get_message()
                except KeepTrying:
                    pass
                else:
                    self.dispatch(func, message)
        except InvalidCommand:
            logger.error("Invalid command received")
            self.client_socket._sock.close()
        except Exception:
            self.client_socket._sock.close()

    def dispatch(self, func, message):
        cmd = func.__name__.upper()
        command = AliveCommand(self, cmd=cmd, origin="%s:%d" % (self.address[0], self.address[1]))

        if func.threaded:
            commands_pool = self.server.pool
            pool_size = self.server.pool_size
            pool_size_warning = self.server.pool_size_warning
            commands_pool.spawn(func, command, self.client_socket, message, command)
            pool_used = len(commands_pool)
            if pool_used >= pool_size_warning:
                logger.warning("Commands pool is close to be full (%s/%s)", pool_used, pool_size)
            elif pool_used == pool_size:
                logger.error("Commands poll is full! (%s/%s)", pool_used, pool_size)
        else:
            try:
                command.executed(func(message))
            except (IOError, RuntimeError, socket.error) as e:
                command.error(e)

    def send_message(self, cmd, message):
        self.send(chr(cmd) + encode_length(len(message)) + message)

    def close(self):
        self.closed = True

    def reply_update(self):
        self.msg_update(None)

    @command
    def msg_allterms(self, message):
        prefix = message
        prev = ''
        with self.server.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            for t in db.allterms(prefix):
                message = b''
                message += encode_length(t.termfreq)
                current = t.term
                common = os.path.commonprefix([prev, current])
                common_len = len(common)
                message += chr(common_len)
                message += current[common_len:]
                prev = current[:255]
                self.send_message(REPLY.REPLY_ALLTERMS, message)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_collfreq(self, message):
        pass

    @command
    def msg_document(self, message):
        with self.server.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            did = decode_length(message)[0]
            document = db.get_document(did)
            self.send_message(REPLY.REPLY_DOCDATA, document.get_data())
            for i in document.values():
                message = b''
                message += encode_length(i.num)
                message += i.value
                self.send_message(REPLY.REPLY_VALUE, message)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_termexists(self, message):
        pass

    @command
    def msg_termfreq(self, message):
        pass

    @command
    def msg_valuestats(self, message):
        pass

    @command
    def msg_keepalive(self, message):
        pass

    @command
    def msg_doclength(self, message):
        pass

    @command
    def msg_query(self, message):
        pass

    @command
    def msg_termlist(self, message):
        prev = ''
        with self.server.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            did = decode_length(message)[0]
            document = db.get_document(did)
            self.send_message(REPLY.REPLY_DOCLENGTH, encode_length(db.get_doclength(did)))
            for t in db.get_termlist(document):
                message = b''
                message += encode_length(t.wdf)
                message += encode_length(t.termfreq)
                current = t.term
                common = os.path.commonprefix([prev, current])
                common_len = len(common)
                message += chr(common_len)
                message += current[common_len:]
                prev = current[:255]
                self.send_message(REPLY.REPLY_TERMLIST, message)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_positionlist(self, message):
        pass

    @command
    def msg_postlist(self, message):
        pass

    @command
    def msg_reopen(self, message):
        pass

    @command
    def msg_update(self, message):
        """
        REPLY_UPDATE <protocol major version> <protocol minor version> I<db doc count> I(<last docid> - <db doc count>) I<doclen lower bound> I(<doclen upper bound> - <doclen lower bound>) B<has positions?> I<db total length> <UUID>
        """
        XAPIAN_REMOTE_PROTOCOL_MAJOR_VERSION = 38
        XAPIAN_REMOTE_PROTOCOL_MINOR_VERSION = 0
        message = b''
        message += chr(XAPIAN_REMOTE_PROTOCOL_MAJOR_VERSION)
        message += chr(XAPIAN_REMOTE_PROTOCOL_MINOR_VERSION)

        if self.endpoints:
            with self.server.databases_pool.database(self.endpoints, writable=False, create=True) as db:
                num_docs = db.get_doccount()
                doclen_lb = db.get_doclength_lower_bound()
                message += encode_length(num_docs)
                message += encode_length(db.get_lastdocid() - num_docs)
                message += encode_length(doclen_lb)
                message += encode_length(db.get_doclength_upper_bound() - doclen_lb)
                message += (b'1' if db.has_positions() else b'0')
                total_len = int(db.get_avlength() * num_docs + 0.5)
                message += encode_length(total_len)
                uuid = db.get_uuid()
                message += uuid

        self.send_message(REPLY.REPLY_UPDATE, message)

    @command
    def msg_adddocument(self, message):
        pass

    @command
    def msg_cancel(self, message):
        pass

    @command
    def msg_deletedocumentterm(self, message):
        pass

    @command
    def msg_commit(self, message):
        pass

    @command
    def msg_replacedocument(self, message):
        pass

    @command
    def msg_replacedocumentterm(self, message):
        pass

    @command
    def msg_deletedocument(self, message):
        pass

    @command
    def msg_writeaccess(self, message):
        pass

    @command
    def msg_getmetadata(self, message):
        pass

    @command
    def msg_setmetadata(self, message):
        pass

    @command
    def msg_addspelling(self, message):
        pass

    @command
    def msg_removespelling(self, message):
        pass

    @command
    def msg_getmset(self, message):
        pass

    @command
    def msg_shutdown(self, message):
        pass

    @command
    def msg_metadatakeylist(self, message):
        pass

    @command
    def msg_freqs(self, message):
        pass

    @command
    def msg_uniqueterms(self, message):
        pass

    @command
    def msg_select(self, message):
        self.endpoints = [message]
        self.msg_update(None)


class CommandServer(StreamServer):
    pool_size = COMMANDS_POOL_SIZE
    receiver_class = ClientReceiver

    def __init__(self, *args, **kwargs):
        self.databases_pool = kwargs.pop('databases_pool')

        super(CommandServer, self).__init__(*args, **kwargs)

        self.pool_size_warning = int(self.pool_size / 3.0 * 2.0)
        self.pool = ThreadPool(self.pool_size)
        self.clients = set()

    def build_client(self, client_socket, address):
        return self.receiver_class(self, client_socket, address)

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
                logger.error("Forcing server shutdown (%s clients)...", len(self.clients))
        else:
            if max_age is None:
                max_age = 10
            logger.warning("Hitting Ctrl+C again will terminate all running tasks!")
            super(CommandServer, self).close()

        now = time.time()
        clean = []
        for client in self.clients:
            if max_age is None or client.weak_client or now - client.activity > max_age:
                try:
                    # Close underlying client socket
                    client.client_socket._sock.close()
                except AttributeError:
                    pass
                clean.append(client)

        for client in clean:
            self.clients.discard(client)

        return not bool(self.clients)


def xapiand_run(data=None, logfile=None, pidfile=None, uid=None, gid=None, umask=0,
        working_directory=None, verbosity=1, commit_slots=None, commit_timeout=None,
        listener=None, queue_type=None, **options):

    Timeouts = collections.namedtuple('Timeouts', 'timeout commit delayed maximum')
    if commit_timeout is None:
        commit_timeout = COMMIT_TIMEOUT
    timeouts = Timeouts(
        timeout=min(max(int(round(commit_timeout * 0.3)), 1), 3),
        commit=commit_timeout * 1.0,
        delayed=commit_timeout * 3.0,
        maximum=commit_timeout * 9.0,
    )

    databases_pool = DatabasesPool(data=data, log=logger)
    xapian_server = CommandServer(listener, databases_pool=databases_pool)

    gevent.signal(signal.SIGTERM, xapian_server.close)
    gevent.signal(signal.SIGINT, xapian_server.close)

    logger.debug("Starting server at %s..." % listener)
    try:
        xapian_server.start()
    except Exception as exc:
        logger.error("Cannot start server: %s", exc)
        sys.exit(-1)

    logger.info("Waiting for commands...")
    msg = None
    main_queue = queue.Queue()
    while not xapian_server.closed:
        try:
            msg = main_queue.get(True, timeouts.timeout)
        except queue.Empty:
            continue
        if not msg:
            continue

    logger.debug("Waiting for connected clients to disconnect...")
    while True:
        if xapian_server.close(max_age=10):
            break
        if gevent.wait(timeout=3):
            break
