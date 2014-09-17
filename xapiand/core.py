from __future__ import absolute_import, unicode_literals

import os
import re
import time
import logging
import subprocess
from hashlib import md5
from collections import deque
from contextlib import contextmanager

import gevent
from gevent import pool, socket
from gevent.lock import RLock

import xapian

from .exceptions import XapianError, InvalidIndexError
from .serialise import serialise_value, normalize
from .utils import parse_url, build_url
from .platforms import pid_exists

DATABASE_MAX_LIFE = 900  # stop writer adter 15 minutes of inactivity
DATABASE_SHORT_LIFE = max(DATABASE_MAX_LIFE - 60, DATABASE_MAX_LIFE - DATABASE_MAX_LIFE / 3, 0)

MIN_TCP_SERVER_PORTS = 100

DOCUMENT_ID_TERM_PREFIX = 'Q'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'

KEY_RE = re.compile(r'[_a-zA-Z][_a-zA-Z0-9]*')

PREFIX_RE = re.compile(r'(?:([_a-zA-Z][_a-zA-Z0-9]*):)?("[\w.]+"|[\w.]+)')
TERM_SPLIT_RE = re.compile(r'\W')

XAPIAN_PREFER_BRASS = True
XAPIAN_TCPSRV = 'xapian-tcpsrv-1.3'
XAPIAN_TCPSRV_ENV = os.environ.copy()
if XAPIAN_PREFER_BRASS:
    XAPIAN_TCPSRV_ENV['XAPIAN_PREFER_BRASS'] = '1'
elif 'XAPIAN_PREFER_BRASS' in XAPIAN_TCPSRV_ENV:
    del XAPIAN_TCPSRV_ENV['XAPIAN_PREFER_BRASS']


def find_terms(value, field=None):
    for term_field, terms in PREFIX_RE.findall(value):
        if not term_field:
            term_field = field
        for term in TERM_SPLIT_RE.split(terms):
            yield term, term_field, terms


def expand_terms(value, field=None, connector=' AND '):
    all_terms = {}
    for term, term_field, terms in find_terms(value, None):
        if term_field is None or term_field.lower() == term_field:
            all_terms.setdefault((term_field, terms), []).append(term)
    replacements = []
    for (term_field, terms), terms_list in all_terms.items():
        if terms[0] == '"':
            terms_list = [terms]
        if term_field is None:
            term_field = field
            replace_ = terms
        else:
            replace_ = '%s:%s' % (term_field, terms)
        if term_field:
            with_ = connector.join('%s:%s' % (term_field, t) for t in terms_list if t)
            if replace_ != with_:
                replacements.append((replace_, with_, len(terms_list) > 1))
    for replace_, with_, parenthesis in replacements:
        if parenthesis and len(replacements) > 1:
            with_ = '(' + with_ + ')'
        value = value.replace(replace_, with_)
    return value


def get_slot(name):
    if KEY_RE.match(name):
        _name = name.lower()
        if _name != name:
            _name = name.upper()
        return int(md5(_name).hexdigest(), 16) & 0xffffffff


def _spawn_tcpservers(endpoints, data='.', log=logging):
    from . import Xapian

    max_pool_size = 10

    all_servers = {}
    p = pool.Pool(max_pool_size)

    def spawner(db, parse, data, log):
        scheme, hostname, port, username, password, path, query, query_dict = parse
        servers = '%s:%s' % (hostname, port or 8890)
        xapiand = all_servers.setdefault(servers, Xapian(
            servers,
            max_pool_size=max_pool_size,
            max_retries=0,
            max_connect_retries=1,
            socket_timeout=1,
            weak=True,
            socket_class=socket.socket,
        ))
        time_, address = xapiand.spawn(db)
        server = TcpDatabase(db, None, address)
        server.time = time_
        return server

    def _port(db):
        if db.startswith('xapian://'):
            try:
                xapian_spawn(db, spawner)
            except InvalidIndexError as exc:
                log.error("%s", exc)

    jobs = [p.spawn(_port, db) for db in endpoints]
    gevent.joinall(jobs)


def _xapian_subdatabase(subdatabases, db, writable, create, data='.', log=logging):
    parse = parse_url(db)
    scheme, hostname, port, username, password, path, query, query_dict = parse
    key = (scheme, hostname, port, username, password, path)
    try:
        return subdatabases[(writable, key)], False
    except KeyError:
        if path and not path.startswith('/'):
            path = os.path.join(data, path)
        if scheme == 'file':
            database = _xapian_database_open(path, writable, create, data, log)
        elif scheme == 'xapian':
            if path:
                try:
                    server = tcpservers[db]
                    if not server.active:
                        raise KeyError
                    hostname, port = server.address
                except KeyError:
                    raise InvalidIndexError("Cannot connect to TCP server")
            timeout = int(query_dict.get('timeout', 0))
            database = _xapian_database_connect(hostname, port or 8891, timeout, writable, data, log)
        else:
            raise InvalidIndexError("Invalid database scheme")
        database._db = db
        subdatabases[(writable, key)] = database

        log.debug("%s %s: %s", "Writable endpoint" if writable else "Endpoint", "used" if create else "opened", database._db)
        return database, True


def _xapian_database_open(path, writable, create, data='.', log=logging):
    try:
        if create:
            try:
                directory = os.path.dirname(path)
                if directory and not os.path.isdir(directory):
                    os.makedirs(directory, 0700)
            except OSError:
                pass
        if writable:
            database = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN if create else xapian.DB_OPEN)
        else:
            try:
                database = xapian.Database(path)
            except xapian.DatabaseError:
                if create:
                    database = xapian.WritableDatabase(path, xapian.DB_CREATE_OR_OPEN)
                    database.close()
                database = xapian.Database(path)
    except xapian.DatabaseLockError as exc:
        raise InvalidIndexError("Unable to lock index at %s: %s" % (path, exc))
    except xapian.DatabaseOpeningError as exc:
        raise InvalidIndexError("Unable to open index at %s: %s" % (path, exc))
    except xapian.DatabaseError as exc:
        raise InvalidIndexError("Unable to use index at %s: %s" % (path, exc))
    return database


def _xapian_database_connect(host, port, timeout, writable, data='.', log=logging):
    try:
        if writable:
            database = xapian.remote_open_writable(host, port, timeout)
        else:
            database = xapian.remote_open(host, port, timeout)
        database.keep_alive()
    except xapian.NetworkError:
        raise InvalidIndexError("Unable to connect to index at %s:%s" % (host, port))
    return database


def _xapian_database(endpoints, writable, create, data='.', log=logging):
    missing = []
    with tcpservers.lock:
        now = time.time()
        for db in endpoints:
            try:
                server = tcpservers[db]
                if not server.process:
                    raise KeyError
                server.time = now
            except KeyError:
                missing.append(db)
    _spawn_tcpservers(missing, data=data, log=log)

    if writable:
        database = xapian.WritableDatabase()
    else:
        database = xapian.Database()

    databases = len(endpoints)
    _all_databases = [None] * databases
    _all_databases_config = [None] * databases

    database._all_databases = _all_databases
    database._all_databases_config = _all_databases_config
    database._endpoints = endpoints
    database._subdatabases = {}

    database._closed = False

    for subdatabase_number, db in enumerate(endpoints):
        if database._all_databases[subdatabase_number] is None:
            _database, _ = _xapian_subdatabase(database._subdatabases, db, writable, create, data, log)
            database._all_databases[subdatabase_number] = _database
            database._all_databases_config[subdatabase_number] = (db, writable, create)
            if _database:
                database.add_database(_database)

    database._db = " ".join(d._db for d in database._all_databases if d)
    log.debug("%s %s: %s", "Writable database" if writable else "Database", "used" if create else "opened", database._db)
    return database


def _xapian_spawn(address, path, data='.', log=logging):
    args = [XAPIAN_TCPSRV, '--interface=%s' % address[0], '--port=%s' % address[1], '--timeout=0', '--writable', '--quiet', path]
    FNULL = open(os.devnull, 'w')
    try:
        process = subprocess.Popen(args, stdout=FNULL, stderr=subprocess.STDOUT, env=XAPIAN_TCPSRV_ENV)
        log.info("Spawned xapian TCP server for \"%s\": %s:%s (pid:%s)", path, address[0], address[1], process.pid)
        return process
    except Exception as exc:
        log.error("Can't exec %r: %s", ' '.join(args), exc)
        raise IOError("Cannot spawn xapian TCP server process")
    finally:
        gevent.sleep(0.1)


def _xapian_spawner(db, parse, data='.', log=logging):
    scheme, hostname, port, username, password, path, query, query_dict = parse
    address = ('0.0.0.0', tcpservers.acquire())
    process = _xapian_spawn(address, path, data=data, log=log)
    # if address[1] == 8900: address = (address[0], 8990)  # port forwarder enabled
    server = TcpDatabase(db, process, address)
    return server


def xapian_spawn(db, spawner=_xapian_spawner, data='.', log=logging):
    parse = parse_url(db)
    db = build_url(*parse)
    scheme, hostname, port, username, password, path, query, query_dict = parse
    try:
        with tcpservers.lock:
            server = tcpservers[db]
            if not server.active:
                raise KeyError
    except KeyError:
        try:
            server = spawner(db, parse, data=data, log=log)
            tcpservers.setdefault(db, server)
        except Exception as exc:
            try:
                server = tcpservers[db]
                if not server.active:
                    raise KeyError
            except KeyError:
                raise InvalidIndexError("Cannot spawn TCP server: %s" % exc)
    if server.process:
        server.time = time.time()
    return server.time, server.address


class CleanableObject(object):
    def __init__(self):
        self.lock = RLock()
        self.time = time.time()
        self.used = False
        self.cleaned = False

    def __del__(self):
        self.cleanup()

    def cleanup(self, data='.', log=logging):
        # self.cleaned = True
        raise NotImplementedError


class CleanablePool(dict):
    def __init__(self, *args, **kwargs):
        super(CleanablePool, self).__init__(*args, **kwargs)
        self.lock = RLock()
        self.time = time.time()

    def cleanup(self, timeout, data='.', log=logging):
        """
        Removes old timedout databases from the pool.

        """
        now = time.time()
        if now - self.time < timeout:
            return

        with self.lock:
            cleanups = []
            for key, obj in list(self.items()):
                if not obj.used and now - obj.time > timeout:
                    cleanups.append(obj)
                    del self[key]
            self.time = now

        for obj in cleanups:
            obj.cleanup()


class TcpDatabase(CleanableObject):
    def __init__(self, database, process, address):
        super(TcpDatabase, self).__init__()
        self.database = database
        self.process = process
        self.address = address

    @property
    def active(self):
        if self.process and not pid_exists(self.process.pid):
            return False
        if time.time() - self.time > DATABASE_SHORT_LIFE:
            return False
        return True

    def cleanup(self, data='.', log=logging):
        if not self.cleaned:
            if self.process:
                self.process.kill()
                tcpservers.release(self.address[1])
                log.info("Stopped xapian TCP server: %s:%s (pid:%s).", self.address[0], self.address[1], self.process.pid)
            self.cleaned = True


class DatabasesPoolQueue(CleanableObject):
    def __init__(self):
        super(DatabasesPoolQueue, self).__init__()
        self.unused = deque()
        self.used = set()

    def cleanup(self, data='.', log=logging):
        if not self.cleaned:
            for database in self.unused:
                xapian_close(database, data=data, log=log)
            self.cleaned = True


class DatabasesPool(CleanablePool):
    pass


class TcpPool(CleanablePool):
    def __init__(self, *args, **kwargs):
        super(TcpPool, self).__init__(*args, **kwargs)
        self.unused = deque()
        self.used = set()
        self.port = 8900

    def acquire(self):
        with self.lock:
            try:
                if len(self.used) < MIN_TCP_SERVER_PORTS:
                    raise IndexError
                port = self.unused.pop()
            except IndexError:
                port = self.port
                self.port += 1
            self.used.add(port)
        return port

    def release(self, port):
        with self.lock:
            self.used.discard(port)
            self.unused.append(port)


@contextmanager
def xapian_database(databases_pool, endpoints, writable, create=False, reopen=False, data='.', log=logging):
    """
    Returns a xapian.Database with multiple endpoints attached.

    """
    database = None
    new = False
    endpoints = tuple(build_url(*parse_url(db.strip())) for db in endpoints)

    with databases_pool.lock:
        pool_queue = databases_pool.setdefault((writable, endpoints), DatabasesPoolQueue())
        with pool_queue.lock:
            try:
                database = pool_queue.unused.pop()
                pool_queue.used.add(database)
            except IndexError:
                new = True
            pool_queue.time = time.time()

    try:
        if new:
            database = _xapian_database(endpoints, writable, create, data=data, log=log)
            pool_queue.used.add(database)
        if reopen:
            database = xapian_reopen(database, data=data, log=log)

        yield database

    finally:
        with pool_queue.lock:
            if database:
                pool_queue.used.discard(database)
                if len(pool_queue.unused) < 10:
                    if not database._closed:
                        pool_queue.unused.append(database)
                else:
                    xapian_close(database)
            pool_queue.time = time.time()


def xapian_close(database, data='.', log=logging):
    subdatabases = database._subdatabases

    # Could not be opened, try full reopen:
    endpoints = database._endpoints
    writable = isinstance(database, xapian.WritableDatabase)

    # Remove database from pool
    _database = subdatabases.pop((writable, endpoints), None)
    assert not _database or _database == database
    # ...and close.
    if database:
        database.close()

    # Subdatabases cleanup:
    for subdatabase in database._all_databases:
        subdatabase_number = database._all_databases.index(subdatabase)
        db, writable, create = database._all_databases_config[subdatabase_number]
        scheme, hostname, port, username, password, path, query, query_dict = parse_url(db)
        key = (scheme, hostname, port, username, password, path)

        # Remove subdatabase from pool
        _subdatabase = subdatabases.pop((writable, key), None)
        assert not _subdatabase or _subdatabase == subdatabase
        # ...and close (close on the main database should have already closed it anyway).
        if subdatabase:
            subdatabase.close()

    database._closed = True
    log.debug("Database %s: %s", "closed", database._db)


def xapian_reopen(database, data='.', log=logging):
    try:
        database.reopen()
    except (xapian.DatabaseOpeningError, xapian.NetworkError) as exc:
        log.error("xapian_reopen database: %s", exc)

        # Could not be opened, try full reopen:
        xapian_close(database)

        endpoints = database._endpoints
        writable = isinstance(database, xapian.WritableDatabase)
        database = _xapian_database(endpoints, writable, False, data=data, log=log)

    return database


def xapian_index(database, document, commit=False, data='.', log=logging):
    document_id, document_values, document_terms, document_texts, document_data, default_language, default_spelling, default_positions = document

    document = xapian.Document()

    if document_data:
        document.set_data(document_data)

    for name, value in (document_values or {}).items():
        name = name.strip()
        slot = get_slot(name)
        if slot:
            value = serialise_value(value)[0]
            if value:
                document.add_value(slot, value)
        else:
            log.warning("Ignored document value name (%r)", name)

    if isinstance(document_id, basestring):
        document.add_value(get_slot('id'), document_id)
        document_id = DOCUMENT_ID_TERM_PREFIX + document_id
        document.add_boolean_term(document_id)  # Make sure document_id is also a term (otherwise it doesn't replace an existing document)

    for terms in document_terms or ():
        if isinstance(terms, (tuple, list)):
            terms, weight, prefix, position = (list(terms) + [None] * 4)[:4]
        else:
            weight = prefix = position = None
        if not terms:
            continue

        weight = 1 if weight is None else weight
        prefix = '' if prefix is None else prefix

        for term, field, terms in find_terms(terms, None):
            if field:
                lower = field.lower() == field
                term_prefix = '%s%s' % (DOCUMENT_CUSTOM_TERM_PREFIX, get_slot(field))
            else:
                lower = False
                term_prefix = prefix
            for term in serialise_value(term):
                if term:
                    if lower:
                        term = term.lower()
                    if position is None:
                        document.add_term(term_prefix + term, weight)
                    else:
                        document.add_posting(term_prefix + term, position, weight)

    for text in document_texts or ():
        if isinstance(text, (tuple, list)):
            text, weight, prefix, language, spelling, positions = (list(text) + [None] * 6)[:6]
        else:
            weight = prefix = language = spelling = positions = None
        if not text:
            continue

        weight = 1 if weight is None else weight
        prefix = '' if prefix is None else prefix
        language = default_language if language is None else language
        positions = default_positions if positions is None else positions
        spelling = default_spelling if spelling is None else spelling

        term_generator = xapian.TermGenerator()
        term_generator.set_database(database)
        term_generator.set_document(document)
        if language:
            term_generator.set_stemmer(xapian.Stem(language))
        if positions:
            index_text = term_generator.index_text
        else:
            index_text = term_generator.index_text_without_positions
        if spelling:
            term_generator.set_flags(xapian.TermGenerator.FLAG_SPELLING)
        index_text(normalize(text), weight, prefix)

    return xapian_replace(database, document_id, document)


def xapian_replace(database, document_id, document, commit=False, data='.', log=logging):
    try:
        docid = database.replace_document(document_id, document)
    except xapian.InvalidArgumentError as exc:
        log.error("%s", exc)
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
        try:
            docid = database.replace_document(document_id, document)
        except xapian.InvalidArgumentError as exc:
            log.error("%s", exc)
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    if commit:
        database = xapian_commit(database, data=data, log=log)
    return database, docid


def xapian_delete(database, document_id, commit=False, data='.', log=logging):
    try:
        database.delete_document(document_id)
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
        try:
            database.delete_document(document_id)
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    if commit:
        database = xapian_commit(database, data=data, log=log)
    return database


def xapian_commit(database, data='.', log=logging):
    try:
        database.commit()
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
        try:
            database.commit()
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    log.debug("Commit executed: %s", database._db)
    return database


def xapian_cleanup(databases_pool, timeout, data='.', log=logging):
    tcpservers.cleanup(timeout, data=data, log=log)
    databases_pool.cleanup(timeout, data=data, log=log)


tcpservers = TcpPool()


def get_document(database, docid, data='.', log=logging):
    try:
        document = database.get_document(docid)
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
        try:
            document = database.get_document(docid)
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    return database, document


def get_value(database, document, slot, data='.', log=logging):
    try:
        value = document.get_value(slot)
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
            document = database.get_document(document.get_docid())
        try:
            value = document.get_value(slot)
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    return database, value


def get_data(database, document, data='.', log=logging):
    try:
        _data = document.get_data()
    except xapian.DocNotFoundError:
        return
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
            document = database.get_document(document.get_docid())
        try:
            _data = document.get_data()
        except xapian.DocNotFoundError:
            return
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    return database, _data


def get_termlist(database, document, data='.', log=logging):
    try:
        termlist = document.termlist()
    except (xapian.NetworkError, xapian.DatabaseError):
        _database = xapian_reopen(database, data=data, log=log)
        if database != _database:
            database = _database
            document = database.get_document(document.get_docid())
        try:
            termlist = document.termlist()
        except (xapian.NetworkError, xapian.DatabaseError) as exc:
            raise XapianError(exc)
    return database, termlist
