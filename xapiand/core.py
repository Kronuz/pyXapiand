from __future__ import absolute_import, unicode_literals

import os
import re
import logging
from hashlib import md5

import xapian

from .exceptions import InvalidIndexError
from .serialise import serialise_value, normalize
from .utils import parse_url, build_url


KEY_RE = re.compile(r'[_a-zA-Z][_a-zA-Z0-9]*')


def get_slot(name):
    if KEY_RE.match(name):
        return int(md5(name.lower()).hexdigest(), 16) & 0xffffffff


def _xapian_subdatabase(databases_pool, db, writable, create, data='.', log=logging):
    parse = parse_url(db)
    scheme, hostname, port, username, password, path, query, query_dict = parse
    key = (scheme, hostname, port, username, password, path)
    try:
        return databases_pool[(writable, key)], False
    except KeyError:
        if scheme == 'file':
            database = _xapian_database_open(databases_pool, path, writable, create, data, log)
        elif scheme == 'xapian':
            timeout = int(query_dict.get('timeout', 0))
            database = _xapian_database_connect(databases_pool, hostname, port or 33333, timeout, writable, data, log)
        database._db = db
        databases_pool[(writable, key)] = database
        log.debug("Subdatabase %s: %s", 'created' if create else 'opened', database._db)
        return database, True


def _xapian_database_open(databases_pool, path, writable, create, data='.', log=logging):
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


def _xapian_database_connect(databases_pool, host, port, timeout, writable, data='.', log=logging):
    try:
        if writable:
            database = xapian.remote_open_writable(host, port, timeout)
        else:
            database = xapian.remote_open(host, port, timeout)
    except xapian.NetworkError:
        raise InvalidIndexError(u'Unable to connect to index at %s:%s' % (host, port))
    return database


def _xapian_database(databases_pool, endpoints, writable, create, data='.', log=logging):
    try:
        return databases_pool[(writable, endpoints)], False
    except KeyError:
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
        database._databases_pool = databases_pool
        for subdatabase_number, db in enumerate(endpoints):
            if database._all_databases[subdatabase_number] is None:
                _database, _ = _xapian_subdatabase(databases_pool, db, writable, create, data, log)
                database._all_databases[subdatabase_number] = _database
                database._all_databases_config[subdatabase_number] = (db, writable, create)
                if _database:
                    database.add_database(_database)
        database._db = ' '.join(d._db for d in database._all_databases if d)
        databases_pool[(writable, endpoints)] = database
        log.debug("Databases %s: %s", 'created' if create else 'opened', database._db)
        return database, True


def xapian_database(databases_pool, endpoints, writable, create=False, data='.', log=logging):
    """
    Returns a xapian.Database with multiple endpoints attached.

    """
    endpoints = tuple(build_url(*parse_url(db.strip())) for db in endpoints)
    database, fresh = _xapian_database(databases_pool, endpoints, writable, create, data=data, log=log)
    if not writable and not fresh:  # Make sure we always read the latest:
        database = xapian_reopen(database, data=data, log=log)
    return database


def xapian_reopen(database, data='.', log=logging):
    databases_pool = database._databases_pool

    try:
        database.reopen()
    except (xapian.DatabaseOpeningError, xapian.NetworkError) as e:
        log.error("xapian_reopen database: %s", e)
        # Could not be opened, try full reopen:
        endpoints = database._endpoints
        writable = isinstance(database, xapian.WritableDatabase)

        # Remove database from pool
        _database = databases_pool.pop((writable, endpoints), None)
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
            _subdatabase = databases_pool.pop((writable, key), None)
            assert not _subdatabase or _subdatabase == subdatabase
            # ...and close (close on the main database should have already closed it anyway).
            if subdatabase:
                subdatabase.close()

        database, _ = _xapian_database(databases_pool, endpoints, writable, False, data=data, log=log)

    return database


def xapian_index(databases_pool, db, document, commit=False, data='.', log=logging):
    db = build_url(*parse_url(db.strip()))
    subdatabase, _ = _xapian_subdatabase(databases_pool, db, True, False, data, log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    document_id, document_values, document_terms, document_texts, document_data, default_language, default_spelling, default_positions = document

    document = xapian.Document()

    for name, value in (document_values or {}).items():
        name = name.strip().lower()
        slot = get_slot(name)
        if slot:
            value = serialise_value(value)[0]
            if value:
                document.add_value(slot, value)
        else:
            log.warning("Ignored document value name (%r)", name)

    if isinstance(document_id, basestring):
        document.add_boolean_term(document_id)  # Make sure document_id is also a term (otherwise it doesn't replace an existing document)

    for term in document_terms or ():
        if isinstance(term, (tuple, list)):
            term, weight, prefix, position = (list(term) + [None] * 4)[:4]
        else:
            weight = prefix = position = None
        if not term:
            continue

        weight = 1 if weight is None else weight
        prefix = '' if prefix is None else prefix

        for term in serialise_value(term):
            if term:
                if position is None:
                    document.add_term(prefix + term, weight)
                else:
                    document.add_posting(prefix + term, position, weight)

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


def xapian_delete(databases_pool, db, document_id, commit=False, data='.', log=logging):
    db = build_url(*parse_url(db))
    subdatabase, _ = _xapian_subdatabase(databases_pool, db, True, False, data=data, log=log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    subdatabase.delete_document(document_id)

    if commit:
        subdatabase.commit()


def xapian_commit(databases_pool, db, data='.', log=logging):
    db = build_url(*parse_url(db))
    subdatabase, _ = _xapian_subdatabase(databases_pool, db, True, False, data=data, log=log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    subdatabase.commit()
