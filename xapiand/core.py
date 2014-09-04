from __future__ import absolute_import, unicode_literals

import re
from hashlib import md5

import xapian

from .exceptions import InvalidIndexError
from .serialise import serialise_value, normalize
from .utils import parse_url

KEY_RE = re.compile(r'[_a-zA-Z][_a-zA-Z0-9]*')
XAPIAN_REMOTE_RE = re.compile('xapian://')


def _xapian_database(databases_pool, endpoints, writable, create, data='.', log=None):
    if endpoints in databases_pool:
        database = databases_pool[endpoints]
    else:
        if writable:
            database = xapian.WritableDatabase()
        else:
            database = xapian.Database()
        databases = len(endpoints)
        database._all_databases = [None] * databases
        database._all_databases_config = [None] * databases
        database._endpoints = endpoints
        database._databases_pool = databases_pool
        databases_pool[endpoints] = database
        log.debug("Database %s!", 'created' if create else 'opened')
    for subdatabase_number, db in enumerate(endpoints):
        if database._all_databases[subdatabase_number] is None:
            _database = _xapian_subdatabase(databases_pool, db, writable, create, data, log)
            database._all_databases[subdatabase_number] = _database
            database._all_databases_config[subdatabase_number] = (databases_pool, db, writable, create)
            if _database:
                database.add_database(_database)
    return database


def _xapian_subdatabase(databases_pool, db, writable, create, data='.', log=None):
    scheme, hostname, port, username, password, path, query = parse_url(db)
    key = (scheme, hostname, port, username, password, path)
    if key in databases_pool:
        return databases_pool[key]
    if scheme == 'file':
        database = _xapian_database_open(databases_pool, path, writable, create, data, log)
    else:
        if not port:
            if scheme == 'xapian':
                port = 33333
        timeout = int(query.get('timeout', 0))
        database = _xapian_database_connect(databases_pool, hostname, port, timeout, writable, data, log)
    log.debug("Subdatabase %s! %r", 'created' if create else 'opened', key)
    databases_pool[key] = database
    return database


def _xapian_database_open(databases_pool, path, writable, create, data='.', log=None):
    try:
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


def _xapian_database_connect(databases_pool, host, port, timeout, writable, data='.', log=None):
    try:
        if writable:
            database = xapian.remote_open_writable(host, port, timeout)
        else:
            database = xapian.remote_open(host, port, timeout)
    except xapian.NetworkError:
        raise InvalidIndexError(u'Unable to connect to index at %s:%s' % (host, port))
    return database


def xapian_endpoints(paths, locations, timeout):
    endpoints = []
    for path in paths:
        endpoints.append(path)
    for host, port in locations:
        endpoints.append((host, port, timeout))
    return tuple(endpoints)


def xapian_database(databases_pool, endpoints, writable, create=False, data='.', log=None):
    """
    Returns a xapian.Database with multiple endpoints attached.

    """
    database = _xapian_database(databases_pool, endpoints, writable, create, data=data, log=log)
    if not writable:  # Make sure we always read the latest:
        database = xapian_reopen(database, data=data, log=log)
    return database


def xapian_reopen(database, data='.', log=None):
    for subdatabase in database._all_databases:
        if subdatabase:
            try:
                subdatabase.reopen()
            except (xapian.DatabaseOpeningError, xapian.NetworkError) as e:
                log.error("xapian_reopen subdatabase: %s", e)
                subdatabase_number = database._all_databases.index(subdatabase)
                databases_pool, db, writable, create = database._all_databases_config[subdatabase_number]
                scheme, hostname, port, username, password, path, query = parse_url(db)
                key = (scheme, hostname, port, username, password, path)
                databases_pool.pop(key, None)
                subdatabase = _xapian_subdatabase(databases_pool, db, writable, create, data, log)
                database._all_databases[subdatabase_number] = subdatabase

    try:
        database.reopen()
    except (xapian.DatabaseOpeningError, xapian.NetworkError) as e:
        log.error("xapian_reopen database: %s", e)
        # Could not be opened, try full reopen:
        _all_databases = database._all_databases
        _all_databases_config = database._all_databases_config
        _endpoints = database._endpoints
        _databases_pool = database._databases_pool
        _writable = isinstance(database, xapian.WritableDatabase)

        _databases_pool.pop(_endpoints, None)
        database = _xapian_database(_databases_pool, _endpoints, _writable, False, data=data, log=log)

        # Recover subdatabases:
        database._all_databases = _all_databases
        database._all_databases_config = _all_databases_config
        database._endpoints = _endpoints
        database._databases_pool = _databases_pool
        for subdatabase_number, db in enumerate(_endpoints):
            _database = database._all_databases[subdatabase_number]
            if _database:
                database.add_database(_database)

    return database


def xapian_index(databases_pool, db, document, commit=False, data='.', log=None):
    subdatabase = _xapian_subdatabase(databases_pool, db, True, False, data, log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    document_id, document_values, document_terms, document_texts, document_data, default_language, default_spelling, default_positions = document

    document = xapian.Document()

    for name, value in (document_values or {}).items():
        name = name.strip().lower()
        if KEY_RE.match(name):
            slot = int(md5(name.lower()).hexdigest(), 16) & 0xffffffff
            value = serialise_value(value)
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

        term = normalize(serialise_value(term))
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


def xapian_delete(databases_pool, db, document_id, commit=False, data='.', log=None):
    subdatabase = _xapian_subdatabase(databases_pool, db, True, False, data=data, log=log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    subdatabase.delete_document(document_id)

    if commit:
        subdatabase.commit()


def xapian_commit(databases_pool, db, data='.', log=None):
    subdatabase = _xapian_subdatabase(databases_pool, db, True, False, data=data, log=log)
    if not subdatabase:
        log.error("Database is None (db:%s)", db)
        return

    subdatabase.commit()
