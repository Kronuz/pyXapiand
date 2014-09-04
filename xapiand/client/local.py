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
        self._do_init = True

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

    def _search(self, query, get_matches, get_data, get_terms, get_size):
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

    def facets(self, query):
        self._check_db()
        search = self._search('* FACETS %s LIMIT 0' % query, get_matches=False, get_data=False, get_terms=False, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def terms(self, query):
        self._check_db()
        search = self._search(query, get_matches=True, get_data=False, get_terms=True, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def find(self, query):
        self._check_db()
        search = self._search(query, get_matches=True, get_data=False, get_terms=False, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def search(self, query):
        self._check_db()
        search = self._search(query, get_matches=True, get_data=True, get_terms=False, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def count(self, query):
        if query:
            search = self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=True)
            return search.size
        else:
            database = self._get_database()
            size = database.get_doccount()
            return size

    def _delete(self, document_id, commit):
        self._check_db()
        for db in self.endpoints:
            xapian_delete(self.databases_pool, db, commit=commit, data=self.data, log=self.log)

    def delete(self, obj):
        self._delete(obj, False)

    def cdelete(self, obj):
        self._delete(obj, True)

    def _index(self, obj, commit):
        self._check_db()
        result = index_parser(obj)
        if not isinstance(result, tuple):
            return result
        endpoints, document = result
        for db in endpoints or self.endpoints:
            xapian_index(self.databases_pool, db, document, commit=commit, data=self.data, log=self.log)

    def index(self, obj):
        self._index(obj, False)

    def cindex(self, obj):
        self._index(obj, True)

    def commit(self):
        self._check_db()
        for db in self.endpoints:
            xapian_commit(self.databases_pool, db, data=self.data, log=self.log)
