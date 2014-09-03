from __future__ import absolute_import, unicode_literals

from .. import json

from .. import version
from ..core import xapian_index, xapian_commit, xapian_delete, xapian_database, xapian_reopen
from ..parser import index_parser
from ..search import Search
from ..exceptions import XapianError


class Xapian(object):
    def __init__(self, *args, **kwargs):
        self.databases_pool = {}
        self.endpoints = None
        self.log = kwargs.pop('log', None)
        using = kwargs.pop('using', None)
        if using:
            self.using(using)

    def _check_db(self):
        if not self.endpoints:
            raise XapianError("Select a database with the command USING")

    def version(self):
        return version

    def reopen(self):
        self._check_db()
        xapian_reopen(self.database, log=self.log)

    def using(self, endpoints=None):
        if endpoints:
            assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
            self.endpoints = tuple(endpoints)
            self.database = xapian_database({}, self.endpoints, False, log=self.log)

    def _search(self, query, get_matches, get_data, get_terms, get_size):
        self._check_db()
        search = Search(
            self.database,
            query,
            get_matches=get_matches,
            get_data=get_data,
            get_terms=get_terms,
            get_size=get_size,
            data=self.data,
            log=self.log,
        )
        return search

    def terms(self, query):
        search = self._search(query, get_matches=True, get_data=False, get_terms=True, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def find(self, query):
        search = self._search(query, get_matches=True, get_data=False, get_terms=False, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def search(self, query):
        search = self._search(query, get_matches=True, get_data=True, get_terms=False, get_size=False)
        for result in search.results:
            yield json.loads(result)

    def count(self, query):
        if query:
            search = self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=True)
            return search.size
        else:
            size = self.database.get_doccount()
            return size

    def _delete(self, document_id, commit):
        self._check_db()
        for db in self.endpoints:
            xapian_delete(self.databases_pool, db, commit=commit, log=self.log)

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
            xapian_index(self.databases_pool, db, document, commit=commit, log=self.log)

    def index(self, obj):
        self._index(obj, False)

    def cindex(self, obj):
        self._index(obj, True)

    def commit(self):
        self._check_db()
        for db in self.endpoints:
            xapian_commit(self.databases_pool, db, log=self.log)
