from __future__ import absolute_import, unicode_literals

import os
import re
import time
import logging
import threading
from hashlib import md5
from collections import deque
from contextlib import contextmanager

import xapian

from .exceptions import InvalidIndexError
from .serialise import serialise_value, normalize
from .utils import parse_url, build_url


DOCUMENT_ID_TERM_PREFIX = 'Q'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'

KEY_RE = re.compile(r'[_a-zA-Z][_a-zA-Z0-9]*')

PREFIX_RE = re.compile(r'(?:([_a-zA-Z][_a-zA-Z0-9]*):)?("[\w.]+"|[\w.]+)')
TERM_SPLIT_RE = re.compile(r'\W')


def find_terms(value, field=None):
    for term_field, terms in PREFIX_RE.findall(value):
        if not term_field:
            term_field = field
        for term in TERM_SPLIT_RE.split(terms):
            yield term, term_field, terms


def expand_terms(value, field=None, connector=' AND '):
    all_terms = {}
    for term, term_field, terms in find_terms(value, None):
        if term_field.lower() == term_field:
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


def _xapian_subdatabase(subdatabases, db, writable, create, data='.', log=logging):
    parse = parse_url(db)
    scheme, hostname, port, username, password, path, query, query_dict = parse
    key = (scheme, hostname, port, username, password, path)
    try:
        return subdatabases[(writable, key)], False
    except KeyError:
        if scheme == 'file':
            database = _xapian_database_open(path, writable, create, data, log)
        elif scheme == 'xapian':
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
    except xapian.DatabaseLockError as e:
        raise InvalidIndexError('Unable to lock index at %s: %s' % (path, e))
    except xapian.DatabaseOpeningError as e:
        raise InvalidIndexError('Unable to open index at %s: %s' % (path, e))
    except xapian.DatabaseError as e:
        raise InvalidIndexError('Unable to use index at %s: %s' % (path, e))
    return database


def _xapian_database_connect(host, port, timeout, writable, data='.', log=logging):
    try:
        if writable:
            database = xapian.remote_open_writable(host, port, timeout)
        else:
            database = xapian.remote_open(host, port, timeout)
    except xapian.NetworkError:
        raise InvalidIndexError(u'Unable to connect to index at %s:%s' % (host, port))
    return database


def _xapian_database(endpoints, writable, create, data='.', log=logging):
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


class DatabasesPoolQueue(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.time = time.time()
        self.unused = deque()
        self.used = deque()


class DatabasesPool(dict):
    def __init__(self, *args, **kwargs):
        super(DatabasesPool, self).__init__(*args, **kwargs)
        self.lock = threading.RLock()
        self.time = time.time()

    def cleanup(self, timeout, data='.', log=logging):
        """
        Removes old timedout databases from the pool.

        """
        now = time.time()
        if now - self.time < timeout:
            return

        with self.lock:
            removed_queues = []
            for (writable, endpoints), pool_queue in list(self.items()):
                if not len(pool_queue.used) and now - pool_queue.time > timeout:
                    removed_queues.append(pool_queue)
                    del self[(writable, endpoints)]
            self.time = now

        for pool_queue in removed_queues:
            for database in pool_queue.unused:
                xapian_close(database, data=data, log=log)


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
                pool_queue.used.append(database)
            except IndexError:
                new = True
            pool_queue.time = time.time()

    try:
        if new:
            database = _xapian_database(endpoints, writable, create, data=data, log=log)
            pool_queue.used.append(database)
        if reopen:
            database = xapian_reopen(database, data=data, log=log)

        yield database

    finally:
        with pool_queue.lock:
            if database:
                pool_queue.used.remove(database)
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
    except (xapian.DatabaseOpeningError, xapian.NetworkError) as e:
        log.error("xapian_reopen database: %s", e)

        # Could not be opened, try full reopen:
        xapian_close(database)

        endpoints = database._endpoints
        writable = isinstance(database, xapian.WritableDatabase)
        database = _xapian_database(endpoints, writable, False, data=data, log=log)

    return database


def xapian_index(database, db, document, commit=False, data='.', log=logging):
    subdatabases = database._subdatabases

    db = build_url(*parse_url(db.strip()))
    subdatabase, _ = _xapian_subdatabase(subdatabases, db, True, False, data, log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    document_id, document_values, document_terms, document_texts, document_data, default_language, default_spelling, default_positions = document

    document = xapian.Document()

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
        term_generator.set_database(subdatabase)
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

    if document_data:
        document.set_data(document_data)

    try:
        docid = subdatabase.replace_document(document_id, document)
    except xapian.InvalidArgumentError as e:
        log.error(e, exc_info=True)

    if commit:
        subdatabase.commit()

    return docid


def xapian_delete(database, db, document_id, commit=False, data='.', log=logging):
    subdatabases = database._subdatabases

    db = build_url(*parse_url(db))
    subdatabase, _ = _xapian_subdatabase(subdatabases, db, True, False, data=data, log=log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    subdatabase.delete_document(document_id)

    if commit:
        subdatabase.commit()


def xapian_commit(database, db, data='.', log=logging):
    subdatabases = database._subdatabases

    db = build_url(*parse_url(db))
    subdatabase, _ = _xapian_subdatabase(subdatabases, db, True, False, data=data, log=log)
    if not subdatabase:
        log.error("Subdatabase is None: %s", db)
        return

    subdatabase.commit()
    log.debug('Commit executed: %s', db)
