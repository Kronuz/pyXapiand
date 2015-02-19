from __future__ import unicode_literals, absolute_import, print_function

import os
import sys
import math
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
from gevent.lock import Semaphore

import xapian
from ..core import DatabasesPool, DATABASE_MAX_LIFE
from ..utils import parse_url, build_url, format_time
from ..exceptions import XapianError

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
WRITERS_POOL_SIZE = 200
COMMIT_SLOTS = 10
QUEUE_WRITER_THREAD = 'Writer-%s'

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


def base256ify_double(double):
    mantissa, exp = math.frexp(double)
    # mantissa is now in the range [0.5, 1.0)
    exp -= 1
    mantissa = math.ldexp(mantissa, (exp & 7) + 1)
    # mantissa is now in the range [1.0, 256.0)
    exp >>= 3
    return mantissa, exp


def serialise_double(double):
    # First byte:
    #   bit 7 Negative flag
    #   bit 4..6 Mantissa length - 1
    #   bit 0..3 --- 0-13 -> Exponent + 7
    #               \- 14 -> Exponent given by next byte
    #                - 15 -> Exponent given by next 2 bytes
    #
    #  Then optional medium (1 byte) or large exponent (2 bytes, lsb first)
    #
    #  Then mantissa (0 iff value is 0)
    double = float(double)

    negative = 0x80 if double < 0.0 else 0x00
    if negative:
        double = -double

    double, exp = base256ify_double(double)

    result = []
    if exp >= -7 and exp <= 6:
        result.append(chr((exp + 7) | negative))
    elif exp >= -128 and exp < 127:
        result.append(b'\x8e' if negative else b'\x0e')
        result.append(chr(exp + 128))
    elif exp < -32768 or exp > 32767:
        raise ValueError("Insane exponent in floating point number")
    else:
        result.append(b'\x8f' if negative else b'\x0f')
        result.append(chr((exp + 32768) & 0xff))
        result.append(chr((exp + 32768) >> 8))

    n = len(result)

    for b in range(8):
        byte = int(double) & 0xff
        result.append(chr(byte))
        double -= float(byte)
        double *= 256.0
        if not double:
            break

    n = len(result) - n

    if n:
        result[0] = chr(ord(result[0]) | ((n - 1) << 4))

    return ''.join(result)


def unserialise_double(buf):
    if len(buf) < 2:
        raise ValueError("Bad encoded double: insufficient data")

    first = ord(buf[0])
    if first == 0 and buf[1] == '\x00':
        return 0.0, buf[2:]
    buf = buf[1:]

    negative = first & 0x80
    mantissa_len = ((first >> 4) & 0x07) + 1

    exp = first & 0x0f
    if exp >= 14:
        bigexp = ord(buf[0])
        if exp == 15:
            exp = bigexp | (ord(buf[1]) << 8)
            exp -= 32768
            buf = buf[2:]
        else:
            exp = bigexp - 128
            buf = buf[1:]
    else:
        exp -= 7

    if len(buf) < mantissa_len:
        raise ValueError("Bad encoded double: short mantissa")

    double = 0.0
    mantissa = buf[:mantissa_len]
    buf = buf[mantissa_len:]
    for c in reversed(mantissa):
        double *= 0.00390625  # 1 / 256
        double += float(ord(c))
    if exp:
        try:
            double = math.ldexp(double, exp * 8)
        except OverflowError:
            double = float('inf')

    if negative:
        double = -double

    return double, buf


def encode_length(length):
    if length < 255:
        encoded = chr(length)
    else:
        encoded = b'\xff'
        length -= 255
        while True:
            b = length & 0x7f
            length >>= 7
            if length:
                encoded += chr(b)
            else:
                encoded += chr(b | 0x80)
                break
    return encoded


def decode_length(buf):
    length = buf[0]
    buf = buf[1:]
    if length == b'\xff':
        length = 0
        shift = 0
        size = 0
        for ch in buf:
            ch = ord(ch)
            length |= (ch & 0x7f) << shift
            shift += 7
            size += 1
            if ch & 0x80:
                break
        else:
            raise ValueError("Bad encoded length: insufficient data")
        length += 255
        buf = buf[size:]
    else:
        length = ord(length)
    return length, buf


class ClientReceiver(object):
    def __init__(self, dispatcher, client_socket, address):
        self.weak_client = False

        self.closed = False
        self.dispatcher = dispatcher
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
        logger.info("New connection from %s: %s:%d (%d open connections)" % (client.client_id, self.address[0], self.address[1], len(self.dispatcher.clients)))
        self.reply_update()

    def connectionLost(self, client):
        logger.info("Lost connection (%d open connections)" % len(self.dispatcher.clients))

    def get_message(self, required_type=None):
        while True:
            tmp = self.read(1024)
            if not tmp:
                raise ConnectionClosed
            self.buf += tmp
            try:
                func = self.message_type[ord(self.buf[0])]
            except (TypeError, IndexError):
                raise InvalidCommand
            try:
                length, self.buf = decode_length(self.buf[1:])
            except ValueError:
                continue
            message = self.buf[:length]
            if len(message) != length:
                continue
            self.buf = self.buf[length:]
            self.activity = time.time()
            return func, message

    def handle(self):
        try:
            while not self.closed:
                func, message = self.get_message()
                self.dispatch(func, message)
        except InvalidCommand:
            logger.error("Invalid command received")
            self.client_socket._sock.close()
        except ConnectionClosed:
            self.client_socket._sock.close()
        except Exception:
            self.client_socket._sock.close()

    def dispatch(self, func, message):
        cmd = func.__name__.upper()
        command = AliveCommand(self, cmd=cmd, origin="%s:%d" % (self.address[0], self.address[1]))

        if func.threaded:
            commands_pool = self.dispatcher.pool
            pool_size = self.dispatcher.pool_size
            pool_size_warning = self.dispatcher.pool_size_warning
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

    @property
    def databases_pool(self):
        return self.dispatcher.server.databases_pool

    @command
    def msg_allterms(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            prefix = message
            prev = b''
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
    def msg_collfreq(self, term):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            self.send_message(REPLY.REPLY_COLLFREQ, encode_length(db.get_collection_freq(term)))

    @command
    def msg_document(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
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
    def msg_termexists(self, term):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            self.send_message((REPLY.REPLY_TERMEXISTS if db.term_exists(term) else REPLY.REPLY_TERMDOESNTEXIST), b'')

    @command
    def msg_termfreq(self, term):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            self.send_message(REPLY.REPLY_TERMFREQ, encode_length(db.get_termfreq(term)))

    @command
    def msg_valuestats(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            while message:
                slot, message = decode_length(message)
                reply = b''
                reply += encode_length(db.get_value_freq(slot))
                bound = db.get_value_lower_bound(slot)
                reply += encode_length(len(bound))
                reply += bound
                bound = db.get_value_upper_bound(slot)
                reply += encode_length(len(bound))
                reply += bound
                self.send_message(REPLY.REPLY_VALUESTATS, reply)

    @command
    def msg_keepalive(self, message):
        self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_doclength(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            did, message = decode_length(message)
            self.send_message(REPLY.REPLY_DOCLENGTH, encode_length(db.get_doclength(did)))

    @command
    def msg_query(self, message):
        # Unserialise the Query.
        length, message = decode_length(message)

        query = xapian.Query.unserialise(message[:length])
        message = message[length:]

        qlen, message = decode_length(message)

        collapse_max, message = decode_length(message)

        collapse_key = xapian.BAD_VALUENO
        if collapse_max:
            collapse_key, message = decode_length(message)

        if len(message) < 4 or message[0] not in b'012':
            raise XapianError(xapian.NetworkError)

        order = ord(message[1]) - ord('0')
        sort_key, message = decode_length(message[1:])

        if message[0] not in b'0123':
            raise XapianError(xapian.NetworkError)

        sort_value_forward = (ord(message[1]) != ord('0'))

    @command
    def msg_termlist(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            did, message = decode_length(message)
            document = db.get_document(did)
            self.send_message(REPLY.REPLY_DOCLENGTH, encode_length(db.get_doclength(did)))
            prev = b''
            for t in db.get_termlist(document):
                reply = b''
                reply += encode_length(t.wdf)
                reply += encode_length(t.termfreq)
                current = t.term
                common = os.path.commonprefix([prev, current])
                common_len = len(common)
                reply += chr(common_len)
                reply += current[common_len:]
                prev = current[:255]
                self.send_message(REPLY.REPLY_TERMLIST, reply)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_positionlist(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            did, term = decode_length(message)

            lastpos = -1
            for pos in db.positionlist(did, term):
                self.send_message(REPLY.REPLY_POSITIONLIST, encode_length(pos - lastpos - 1))
                lastpos = pos

            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_postlist(self, term):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            termfreq = db.get_termfreq(term)
            collfreq = db.get_collection_freq(term)
            self.send_message(REPLY.REPLY_POSTLISTSTART, encode_length(termfreq) + encode_length(collfreq))

            lastdocid = 0
            for i in db.postlist(term):
                newdocid = i.docid

                reply = b''
                reply += encode_length(newdocid - lastdocid - 1)
                reply += encode_length(i.wdf)

                self.send_message(REPLY.REPLY_POSTLISTITEM, reply)
                lastdocid = newdocid

            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_reopen(self, message):
        self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_update(self, message, db=None):
        """
        REPLY_UPDATE <protocol major version> <protocol minor version> I<db doc count> I(<last docid> - <db doc count>) I<doclen lower bound> I(<doclen upper bound> - <doclen lower bound>) B<has positions?> I<db total length> <UUID>
        """
        XAPIAN_REMOTE_PROTOCOL_MAJOR_VERSION = 38
        XAPIAN_REMOTE_PROTOCOL_MINOR_VERSION = 0
        reply = b''
        reply += chr(XAPIAN_REMOTE_PROTOCOL_MAJOR_VERSION)
        reply += chr(XAPIAN_REMOTE_PROTOCOL_MINOR_VERSION)

        if self.endpoints:
            def get_stats(db):
                num_docs = db.get_doccount()
                doclen_lb = db.get_doclength_lower_bound()
                stats = b''
                stats += encode_length(num_docs)
                stats += encode_length(db.get_lastdocid() - num_docs)
                stats += encode_length(doclen_lb)
                stats += encode_length(db.get_doclength_upper_bound() - doclen_lb)
                stats += (b'1' if db.has_positions() else b'0')
                total_len = int(db.get_avlength() * num_docs + 0.5)
                stats += encode_length(total_len)
                uuid = db.get_uuid()
                stats += uuid
                return stats

            if db:
                reply += get_stats(db)
            else:
                with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
                    reply += get_stats(db)

        self.send_message(REPLY.REPLY_UPDATE, reply)

    @command
    def msg_adddocument(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            did = wdb.add_document(xapian.Document.unserialise(message))
            self.send_message(REPLY.REPLY_ADDDOCUMENT, encode_length(did))

    @command
    def msg_cancel(self, message):
        pass  # TODO: Implement write!

    @command
    def msg_deletedocumentterm(self, term):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            wdb.delete_document(term)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_commit(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            wdb.commit()
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_replacedocument(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            did, message = decode_length(message)
            document = xapian.Document.unserialise(message)
            wdb.replace_document(did, document)

    @command
    def msg_replacedocumentterm(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            length, message = decode_length(message)
            term = message[:length]
            message = message[length:]
            document = xapian.Document.unserialise(message)
            did = wdb.replace_document(term, document)
            self.send_message(REPLY.REPLY_ADDDOCUMENT, encode_length(did))

    @command
    def msg_deletedocument(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            did, message = decode_length(message)
            wdb.delete_document(did)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_writeaccess(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            self.msg_update(None, db=wdb)

    @command
    def msg_getmetadata(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            self.send_message(REPLY.REPLY_METADATA, db.get_metadata(message))

    @command
    def msg_setmetadata(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            keylen, message = decode_length(message)
            key = message[:keylen]
            message = message[keylen:]
            val = message
            wdb.set_metadata(key, val)

    @command
    def msg_addspelling(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            freqinc, word = decode_length(message)
            wdb.add_spelling(word, freqinc)

    @command
    def msg_removespelling(self, message):
        pass  # TODO: Implement write!
        with self.databases_pool.database(self.endpoints, writable=True, create=True) as wdb:
            freqinc, word = decode_length(message)
            wdb.remove_spelling(word, freqinc)

    @command
    def msg_getmset(self, message):
        raise RuntimeError("Unexpected MSG_GETMSET!")

    @command
    def msg_shutdown(self, message):
        raise ConnectionClosed

    @command
    def msg_metadatakeylist(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            prefix = message
            prev = b''
            for t in db.metadata_keys(prefix):
                reply = b''
                current = t.term
                common = os.path.commonprefix([prev, current])
                common_len = len(common)
                reply += chr(common_len)
                reply += current[common_len:]
                prev = current[:255]
                self.send_message(REPLY.REPLY_METADATAKEYLIST, reply)
            self.send_message(REPLY.REPLY_DONE, b'')

    @command
    def msg_freqs(self, term):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            reply = encode_length(db.get_termfreq(term))
            reply += encode_length(db.get_collection_freq(term))
            self.send_message(REPLY.REPLY_FREQS, reply)

    @command
    def msg_uniqueterms(self, message):
        with self.databases_pool.database(self.endpoints, writable=False, create=True) as db:
            did, message = decode_length(message)
            self.send_message(REPLY.REPLY_UNIQUETERMS, encode_length(db.get_unique_terms(did)))

    @command
    def msg_select(self, endpoint):
        self.endpoints = [endpoint]
        self.msg_update(None)


class XapianDispatcher(StreamServer):
    pool_size = COMMANDS_POOL_SIZE
    receiver_class = ClientReceiver

    def __init__(self, server, *args, **kwargs):
        self.server = server
        super(XapianDispatcher, self).__init__(*args, **kwargs)

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
            super(XapianDispatcher, self).close()

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


def database_name(db):
    return QUEUE_WRITER_THREAD % (hash(db) & 0xffffff)


DATABASE_COMMANDS = {
    'INDEX': (
        'index',
        lambda a: a[0][0],
        dict(),
    ),
    'CINDEX': (
        'index',
        lambda a: a[0][0],
        dict(commit=True),
    ),
    'DELETE': (
        'delete',
        lambda a: a[0],
        dict(),
    ),
    'CDELETE': (
        'delete',
        lambda a: a[0],
        dict(commit=True),
    ),
    'COMMIT': (
        'commit',
        lambda a: '',
        dict(),
    ),
}

Timeouts = collections.namedtuple('Timeouts', 'timeout commit delayed maximum')


class XapianServer(object):
    stopped = False

    def __init__(self, listener, commit_timeout=None, commit_slots=None, data='.'):
        self.databases = {}

        self.pool_size = WRITERS_POOL_SIZE
        self.pool_size_warning = int(self.pool_size / 3.0 * 2.0)
        self.writers_pool = ThreadPool(self.pool_size)

        if not commit_slots:
            commit_slots = COMMIT_SLOTS
        self.commit_lock = Semaphore(commit_slots)

        self.listener = listener
        self.data = data

        self.databases_pool = DatabasesPool(data=self.data, log=logger)
        self.xapian_dispatcher = XapianDispatcher(self, listener)

        if commit_timeout is None:
            commit_timeout = COMMIT_TIMEOUT

        self.timeouts = Timeouts(
            timeout=min(max(int(round(commit_timeout * 0.3)), 1), 3),
            commit=commit_timeout * 1.0,
            delayed=commit_timeout * 3.0,
            maximum=commit_timeout * 9.0,
        )

    def _database_command(self, db, cmd, args):
        start = time.time()
        try:
            attr, arg, kwargs = DATABASE_COMMANDS[cmd]
            with self.databases_pool.database((db,), writable=True, create=True) as database:
                docid = getattr(database, attr)(*args, **kwargs)
        except Exception as exc:
            logger.exception("%s", exc)
            raise
        docid = ' -> %s' % docid if docid else ''
        duration = time.time() - start
        logger.debug(
            "Executed command %s(%s)%s ~%s",
            cmd,
            arg(args),
            docid,
            format_time(duration),
        )

    def _database_commit(self, db, to_commit, force=False):
        if not to_commit:
            return

        now = time.time()

        expires = now - self.timeouts.commit
        expires_delayed = now - self.timeouts.delayed
        expires_max = now - self.timeouts.maximum

        for db, (dt0, dt1, dt2) in list(to_commit.items()):
            do_commit = locked = force and self.commit_lock.acquire()  # If forcing, wait for the lock
            if not do_commit:
                do_commit = dt0 <= expires_max
                if do_commit:
                    logger.warning("Commit maximum expiration reached, commit forced! (%s)", db)
            if not do_commit:
                if dt1 <= expires_delayed or dt2 <= expires:
                    do_commit = locked = self.commit_lock.acquire(False)
                    if not locked:
                        logger.warning("Out of commit slots, commit delayed! (%s)", db)
            if do_commit:
                try:
                    self._database_command(db, 'COMMIT', ())
                    del to_commit[db]
                finally:
                    if locked:
                        self.commit_lock.release()

    def _writer_loop(self, db, tq):
        name = database_name(db)
        to_commit = {}

        current_thread = threading.current_thread()
        tid = current_thread.name.rsplit('-', 1)[-1]
        current_thread.name = '%s-%s' % (name[:14], tid)

        start = last = time.time()

        # Create a gevent Queue for this thread from the other tread's Queue
        # (using the raw underlying deque, 'queue'):
        queue = type(tq)(tq.maxsize)
        queue.queue = tq.queue

        database = None

        # Open the database
        try:
            with self.databases_pool.database((db,), writable=True, create=True) as database:
                logger.info("New writer %s: %s", name, db)
                logger.debug("Database UUID: %s", database.get_uuid())
            msg = None
            timeout = self.timeouts.timeout
            while not self.stopped:
                self._database_commit(db, to_commit)

                now = time.time()
                try:
                    msg = queue.get(True, timeout)
                except queue.Empty:
                    if now - last > DATABASE_MAX_LIFE:
                        logger.debug("Writer timeout... stopping!")
                        break
                    continue
                if not msg:
                    continue
                try:
                    cmd, endpoints, args = msg
                except ValueError:
                    logger.error("Wrong command received!")
                    continue

                for _db in endpoints:
                    _db = build_url(*parse_url(_db.strip()))
                    if _db != db:
                        continue

                    last = now
                    self._database_command(db, cmd, args)

                    if cmd in ('INDEX', 'DELETE'):
                        now = time.time()
                        if db in to_commit:
                            to_commit[db] = (to_commit[db][0], to_commit[db][1], now)
                        else:
                            to_commit[db] = (now, now, now)
        except Exception as e:
            logger.error("Writer ERROR: %s", e)
        finally:
            self.databases.pop(db, None)
            logger.info("Writer %s ended! ~ lived for %s", name, format_time(time.time() - start))

    def start(self):
        gevent.signal(signal.SIGTERM, self.xapian_dispatcher.close)
        gevent.signal(signal.SIGINT, self.xapian_dispatcher.close)

        logger.debug("Starting server at %s..." % self.listener)
        try:
            self.xapian_dispatcher.start()
        except Exception as exc:
            logger.error("Cannot start server: %s", exc)
            sys.exit(-1)

        logger.info("Waiting for commands...")
        msg = None
        main_queue = queue.Queue()
        while not self.xapian_dispatcher.closed:
            try:
                msg = main_queue.get(True, self.timeouts.timeout)
            except queue.Empty:
                continue
            if not msg:
                continue

        logger.debug("Waiting for connected clients to disconnect...")
        while True:
            if self.xapian_dispatcher.close(max_age=10):
                break
            if gevent.wait(timeout=3):
                break

        self.stopped = time.time()

    def start_writer(self, db):
        db = build_url(*parse_url(db.strip()))
        name = database_name(db)
        try:
            tq = None
            t, tq = self.databases[db]
            if t.ready():
                raise KeyError
        except KeyError:
            tq = tq or queue.Queue()
            pool_used = len(self.writers_pool)
            if not (self.pool_size_warning - pool_used) % 10:
                logger.warning("Writers pool is close to be full (%s/%s)", pool_used, self.pool_size)
            elif pool_used == self.pool_size:
                logger.error("Writers poll is full! (%s/%s)", pool_used, self.pool_size)
            t = self.writers_pool.spawn(self._writer_loop, db, tq)
            self.databases[db] = (t, tq)
        return db, name, t, tq

    def get_writer(self, db):
        db, name, t, tq = self.start_writer()
        return tq


def xapiand_run(data=None, logfile=None, pidfile=None, uid=None, gid=None, umask=0,
        working_directory=None, verbosity=1, commit_slots=None, commit_timeout=None,
        listener=None, queue_type=None, **options):

    server = XapianServer(listener, commit_timeout=commit_timeout, commit_slots=commit_slots, data=data)
    server.start()
