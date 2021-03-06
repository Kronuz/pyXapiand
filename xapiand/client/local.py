from __future__ import absolute_import, unicode_literals

import logging

from .. import version
from ..core import DatabasesPool
from ..parser import index_parser, search_parser
from ..search import Search
from ..exceptions import XapianError
from ..results import XapianResults


class Xapian(object):
    def __init__(self, *args, **kwargs):
        self._do_create = False
        self._do_reopen = False
        self.active_endpoints = None
        self.data = kwargs.pop('data', '.')
        self.log = kwargs.pop('log', logging)
        self.databases_pool = DatabasesPool(data=self.data, log=self.log)
        using = kwargs.pop('using', None)
        open_ = kwargs.pop('open', None)
        if using:
            self.using(using)
        elif open_:
            self.open(open_)

    def _check_db(self):
        if not self.active_endpoints:
            raise XapianError("Select a database with the command OPEN")

    def _reopen(self, create=False, endpoints=None):
        endpoints = endpoints or self.active_endpoints
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
            self._do_create = True
            self._reopen(endpoints=endpoints)
            self.active_endpoints = endpoints
        else:
            raise XapianError("You must specify a valid endpoint for the database")

    def open(self, endpoints=None):
        if endpoints:
            assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
            endpoints = tuple(endpoints)
            self._do_create = False
            self._reopen(endpoints=endpoints)
            self.active_endpoints = endpoints
        self._check_db()

    def using(self, endpoints=None):
        if endpoints:
            assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
            endpoints = tuple(endpoints)
            self._do_create = True
            self._reopen(endpoints=endpoints)
            self.active_endpoints = endpoints
        self._check_db()

    def _search(self, query, get_matches, get_data, get_terms, get_size):
        reopen, self._do_reopen = self._do_reopen, False
        with self.databases_pool.database(self.active_endpoints, writable=False, create=self._do_create, reopen=reopen) as database:
            search = Search(
                database,
                query,
                get_matches=get_matches,
                get_data=get_data,
                get_terms=get_terms,
                get_size=get_size,
                data=self.data,
                log=self.log)
            return search

    def facets(self, search, *facets, **kwargs):
        self._check_db()

        terms = kwargs.get('terms')
        ranges = kwargs.get('ranges')
        partials = kwargs.get('partials')
        results_class = kwargs.get('results_class', XapianResults)
        query = search_parser(search if isinstance(search, dict) else 'FACETS ' + search)
        query['search'] = '*'
        if facets is not None:
            query['facets'].extend(facets)
        if terms is not None:
            query['terms'] = terms
        if ranges is not None:
            query['ranges'] = ranges
        if partials is not None:
            query['partials'] = partials
        query.pop('first', None)
        query['maxitems'] = 0
        query.pop('sort_by', None)
        search = self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=False)
        return results_class(search.results)

    def terms(self, search=None, terms=None, ranges=None, partials=None, offset=None, limit=None, order_by=None, results_class=XapianResults):
        self._check_db()
        query = search_parser(search if isinstance(search, dict) else 'TERMS ' + search)
        query.pop('facets', None)
        if terms is not None:
            query['terms'] = terms
        if ranges is not None:
            query['ranges'] = ranges
        if partials is not None:
            query['partials'] = partials
        if offset is not None:
            query['first'] = offset
        if limit is not None:
            query['maxitems'] = limit
        if order_by is not None:
            query['sort_by'] = order_by
        search = self._search(query, get_matches=True, get_data=False, get_terms=True, get_size=True)
        return results_class(search.results)

    def find(self, search=None, facets=None, terms=None, ranges=None, partials=None, offset=None, limit=None, order_by=None, results_class=XapianResults):
        self._check_db()
        query = search_parser(search)
        if facets is not None:
            query['facets'] = facets
        if terms is not None:
            query['terms'] = terms
        if ranges is not None:
            query['ranges'] = ranges
        if partials is not None:
            query['partials'] = partials
        if offset is not None:
            query['first'] = offset
        if limit is not None:
            query['maxitems'] = limit
        if order_by is not None:
            query['sort_by'] = order_by
        search = self._search(query, get_matches=True, get_data=False, get_terms=False, get_size=True)
        return results_class(search.results)

    def search(self, search=None, facets=None, terms=None, ranges=None, partials=None, offset=None, limit=None, order_by=None, results_class=XapianResults):
        self._check_db()
        query = search_parser(search)
        if facets is not None:
            query['facets'] = facets
        if terms is not None:
            query['terms'] = terms
        if ranges is not None:
            query['ranges'] = ranges
        if partials is not None:
            query['partials'] = partials
        if offset is not None:
            query['first'] = offset
        if limit is not None:
            query['maxitems'] = limit
        if order_by is not None:
            query['sort_by'] = order_by
        search = self._search(query, get_matches=True, get_data=True, get_terms=False, get_size=True)
        return results_class(search.results)

    def count(self, search=None, terms=None, ranges=None, partials=None):
        if search or terms or partials:
            query = search_parser(search)
            query.pop('facets', None)
            if terms is not None:
                query['terms'] = terms
            if ranges is not None:
                query['ranges'] = ranges
            if partials is not None:
                query['partials'] = partials
            query.pop('first', None)
            query['maxitems'] = 0
            query.pop('sort_by', None)
            search = self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=True)
            search.get_results().next()
            size = search.estimated
            return size
        else:
            reopen, self._do_reopen = self._do_reopen, False
            with self.databases_pool.database(self.active_endpoints, writable=False, create=self._do_create, reopen=reopen) as database:
                size = database.get_doccount()
                return size

    def _delete(self, id, commit):
        self._check_db()
        reopen, self._do_reopen = self._do_reopen, False
        with self.databases_pool.database(self.active_endpoints, writable=True, create=self._do_create, reopen=reopen) as database:
            database.delete(commit=commit)

    def delete(self, id):
        self._delete(id, False)

    def cdelete(self, id):
        self._delete(id, True)

    def _index(self, obj, commit, **kwargs):
        result = index_parser(obj or kwargs)
        if not isinstance(result, tuple):
            return result
        endpoints, document = result
        if not endpoints:
            endpoints = self.active_endpoints
        if not endpoints:
            self._check_db()
        reopen, self._do_reopen = self._do_reopen, False
        with self.databases_pool.database(endpoints, writable=True, create=self._do_create, reopen=reopen) as database:
            database.index(document, commit=commit)

    def index(self, obj=None, **kwargs):
        self._index(obj, False, **kwargs)

    def cindex(self, obj=None, **kwargs):
        obj = obj or kwargs
        self._index(obj, True, **kwargs)

    def commit(self):
        self._check_db()
        reopen, self._do_reopen = self._do_reopen, False
        with self.databases_pool.database(self.active_endpoints, writable=True, create=self._do_create, reopen=reopen) as database:
            database.commit()
