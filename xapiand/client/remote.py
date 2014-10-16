from __future__ import absolute_import, unicode_literals

from .. import json

from ..exceptions import XapianError
from ..parser import search_parser
from ..results import XapianResults

from .connection import Connection, ServerPool, command


def dumps(obj, **kwargs):
    if isinstance(obj, dict):
        obj = dict((k, v) for k, v in obj.items() if v)
    return json.dumps(obj, **kwargs)


class XapianConnection(Connection):
    _endpoints = None

    def get_name(self):
        if self.endpoints:
            return " ".join(self.endpoints)
        else:
            return ""

    def on_connect(self):
        if self._endpoints:
            endpoints, self._endpoints = self._endpoints, None
            self.using(endpoints)

    @command
    def version(self):
        return self._response(self.execute_command('VERSION'))

    @command
    def reopen(self):
        return self._response(self.execute_command('REOPEN'))

    @command
    def create(self, endpoint):
        endpoints = [endpoint]
        if self._endpoints != endpoints:
            self._response(self.execute_command('CREATE', endpoint))
            self._endpoints = endpoints

    @command
    def open(self, endpoints=None):
        if endpoints:
            if self._endpoints != endpoints:
                assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
                self._response(self.execute_command('OPEN', ','.join(endpoints)))
                self._endpoints = endpoints
        else:
            self._response(self.execute_command('OPEN'))

    @command
    def using(self, endpoints=None):
        if endpoints:
            if self._endpoints != endpoints:
                assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
                self._response(self.execute_command('USING', ','.join(endpoints)))
                self._endpoints = endpoints
        else:
            self._response(self.execute_command('USING'))

    def _response(self, line):
        if line.startswith(">>"):
            if line.startswith(">> OK"):
                return line[7:] or None
            if line.startswith(">> ERR"):
                raise XapianError(line[8:])

    def _search(self, cmd, **query):
        line = self.execute_command(cmd, dumps(query, ensure_ascii=False))
        while line:
            response = self._response(line)
            if response:
                break
            yield json.loads(line)
            line = self.read()

    @command
    def facets(self, search, *facets, **kwargs):
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
        results = self._search('FACETS', **query)
        return results_class(results)

    @command
    def terms(self, search=None, terms=None, ranges=None, partials=None, offset=None, limit=None, order_by=None, results_class=XapianResults):
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
        results = self._search('TERMS', **query)
        return results_class(results)

    @command
    def find(self, search=None, facets=None, terms=None, ranges=None, partials=None, offset=None, limit=None, order_by=None, results_class=XapianResults):
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
        results = self._search('FIND', **query)
        return results_class(results)

    @command
    def search(self, search=None, facets=None, terms=None, ranges=None, partials=None, offset=None, limit=None, order_by=None, results_class=XapianResults):
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
        results = self._search('SEARCH', **query)
        return results_class(results)

    @command
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
            search = dumps(query, ensure_ascii=False)
        response = self._response(self.execute_command('COUNT', search))
        return int(response.split()[0])

    @command
    def delete(self, id):
        return self._response(self.execute_command('DELETE', id))

    @command
    def cdelete(self, id):
        return self._response(self.execute_command('CDELETE', id))

    def _index(self, cmd, obj, **kwargs):
        return self._response(self.execute_command(cmd, dumps(obj or kwargs, ensure_ascii=False)))

    @command
    def index(self, obj=None, **kwargs):
        return self._index('INDEX', obj, **kwargs)

    @command
    def cindex(self, obj=None, **kwargs):
        return self._index('CINDEX', obj, **kwargs)

    @command
    def commit(self):
        return self._response(self.execute_command('COMMIT'))

    @command
    def spawn(self, db):
        response = self._response(self.execute_command('SPAWN', db))
        time_, _, address = response.partition(' ')
        host, _, port = address.partition(':')
        time_, address = float(time_), (host, int(port))
        return time_, address

    @command
    def weak(self):
        return self._response(self.execute_command('WEAK'))


class Xapian(ServerPool):
    connection_class = XapianConnection

    def __init__(self, *args, **kwargs):
        self._using = kwargs.pop('using', None)
        self._open = kwargs.pop('open', None)
        self._weak = kwargs.pop('weak', False)
        super(Xapian, self).__init__(*args, **kwargs)

    def call(self, name, *args, **kwargs):
        def callback(xapian):
            if self._weak:
                xapian.weak()
            if self._using:
                xapian.using(self._using)
            elif self._open:
                xapian.open(self._open)
            return getattr(xapian, name)(*args, **kwargs)
        return self(callback)
