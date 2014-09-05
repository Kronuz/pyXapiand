from __future__ import absolute_import, unicode_literals

from .. import json

from .. import version
from ..core import xapian_index, xapian_commit, xapian_delete, xapian_database
from ..parser import index_parser
from ..search import Search
from ..exceptions import XapianError


class Xapian(object):
    def __init__(self, *args, **kwargs):
        self.databases_pool = {}
        self.endpoints = None
        self.data = kwargs.pop('data', '.')
        self.log = kwargs.pop('log', None)
        using = kwargs.pop('using', None)
        if using:
            self.using(using)

    def _check_db(self):
        if not self.endpoints:
            raise XapianError("Select a database with the command USING")

    def _get_database(self, create=False, endpoints=None):
        endpoints = endpoints or self.endpoints
        if endpoints:
            return xapian_database(self.databases_pool, endpoints, False, create, data=self.data, log=self.log)

    def _reopen(self, create=False, endpoints=None):
        self._get_database(create=create, endpoints=endpoints)
        self._do_reopen = False

    def version(self):
        return version

    def reopen(self):
        self._check_db()
        self._reopen()

    def create(self, endpoint):
        endpoint = endpoint.strip()
        if endpoint:
            endpoints = (endpoint,)
            self._reopen(create=True, endpoints=endpoints)
            self.endpoints = endpoints
        else:
            raise XapianError("You must specify a valid endpoint for the database")

    def using(self, endpoints=None):
        if endpoints:
            assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
            endpoints = tuple(endpoints)
            self._reopen(create=False, endpoints=endpoints)
            self.endpoints = endpoints
        self._check_db()
    open = using

    def _search(self, query, get_matches, get_data, get_terms, get_size, facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        if facets:
            query += ' FACETS %s' % ' '.join(facets)
        if terms:
            query += ' TERMS %s' % ' '.join(terms)
        if partial:
            query += ' PARTIAL %s' % ' PARTIAL '.join(partial)
        if search:
            query += ' SEARCH %s' % ' '.join(search)
        if offset:
            query += ' OFFSET %d' % offset
        if limit:
            query += ' LIMIT %d' % limit
        if order_by:
            query += ' ORDER BY %s' % ' '.join(order_by)

        database = self._get_database()

        search = Search(
            database,
            query,
            get_matches=get_matches,
            get_data=get_data,
            get_terms=get_terms,
            get_size=get_size,
            data=self.data,
            log=self.log,
        )

        return search

    def facets(self, query=''):
        self._check_db()
        search = self._search('* FACETS %s LIMIT 0' % query, get_matches=False, get_data=False, get_terms=False, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def terms(self, query='', partial=None, search=None, offset=None, limit=None, order_by=None):
        self._check_db()
        search = self._search(query, get_matches=True, get_data=False, get_terms=True, get_size=False, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by)
        for result in search.results:
            yield json.loads(result)

    def find(self, query='', facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        self._check_db()
        search = self._search(query, get_matches=True, get_data=False, get_terms=False, get_size=False, facets=facets, terms=terms, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by)
        for result in search.results:
            yield json.loads(result)

    def search(self, query='', facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        self._check_db()
        search = self._search(query, get_matches=True, get_data=True, get_terms=False, get_size=False, facets=facets, terms=terms, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by)
        for result in search.results:
            yield json.loads(result)

    def count(self, query=''):
        if query:
            search = self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=True)
            return search.size
        else:
            database = self._get_database()
            size = database.get_doccount()
            return size

    def _delete(self, id, commit):
        self._check_db()
        for db in self.endpoints:
            xapian_delete(self.databases_pool, db, commit=commit, data=self.data, log=self.log)

    def delete(self, id):
        self._delete(id, False)

    def cdelete(self, id):
        self._delete(id, True)

    def _index(self, obj, commit):
        result = index_parser(obj)
        if not isinstance(result, tuple):
            return result
        endpoints, document = result
        if not endpoints:
            endpoints = self.endpoints
        if not endpoints:
            self._check_db()
        for db in endpoints:
            xapian_index(self.databases_pool, db, document, commit=commit, data=self.data, log=self.log)

    def index(self, obj=None, **kwargs):
        self._index(obj or kwargs, False)

    def cindex(self, obj=None, **kwargs):
        obj = obj or kwargs
        self._index(obj or kwargs, True)

    def commit(self):
        self._check_db()
        for db in self.endpoints:
            xapian_commit(self.databases_pool, db, data=self.data, log=self.log)
