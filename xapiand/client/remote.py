from __future__ import absolute_import, unicode_literals

from .. import json

from ..exceptions import XapianError
from ..parser import search_parser

from .connection import Connection, ServerPool, command


class XapianConnection(Connection):
    def on_connect(self):
        endpoints = getattr(self.context, 'endpoints', None)
        if endpoints:
            self.using(endpoints)

    @command
    def version(self):
        return self._response(self.execute_command('VERSION'))

    @command
    def reopen(self):
        return self._response(self.execute_command('REOPEN'))

    @command
    def create(self, endpoint):
        response = self._response(self.execute_command('CREATE', endpoint))
        self.context.endpoints = [endpoint]
        return response

    @command
    def using(self, endpoints=None):
        if endpoints:
            assert isinstance(endpoints, (list, tuple)), "Endpoints must be a tuple"
            response = self._response(self.execute_command('USING', ','.join(endpoints)))
            self.context.endpoints = endpoints
        else:
            response = self._response(self.execute_command('USING'))
        return response

    def _response(self, line):
        if line.startswith(">>"):
            line = line.decode(self.encoding)
            if line.startswith(">> OK"):
                return line[7:] or None
            if line.startswith(">> ERR"):
                raise XapianError(line[8:])

    def _search(self, cmd, **query):
        line = self.execute_command(cmd, json.dumps(query, ensure_ascii=False))
        while line:
            response = self._response(line)
            if response:
                break
            yield json.loads(line)
            line = self.read()

    @command
    def facets(self, search, *facets, **kwargs):
        terms = kwargs.get('terms')
        partials = kwargs.get('partials')
        query = search_parser(search if isinstance(search, dict) else 'FACETS ' + search)
        query['search'] = '*'
        if facets is not None:
            query['facets'].extend(facets)
        if terms is not None:
            query['terms'] = terms
        if partials is not None:
            query['partials'] = partials
        del query['first']
        query['maxitems'] = 0
        del query['sort_by']
        results = self._search('FACETS', **query)
        for result in results:
            yield result

    @command
    def terms(self, search=None, terms=None, partials=None, offset=None, limit=None, order_by=None):
        query = search_parser(search if isinstance(search, dict) else 'TERMS ' + search)
        del query['facets']
        if terms is not None:
            query['terms'] = terms
        if partials is not None:
            query['partials'] = partials
        if offset is not None:
            query['first'] = offset
        if limit is not None:
            query['maxitems'] = limit
        if order_by is not None:
            query['sort_by'] = order_by
        results = self._search('TERMS', **query)
        for result in results:
            yield result

    @command
    def find(self, search=None, facets=None, terms=None, partials=None, offset=None, limit=None, order_by=None):
        query = search_parser(search)
        if facets is not None:
            query['facets'] = facets
        if terms is not None:
            query['terms'] = terms
        if partials is not None:
            query['partials'] = partials
        if offset is not None:
            query['first'] = offset
        if limit is not None:
            query['maxitems'] = limit
        if order_by is not None:
            query['sort_by'] = order_by
        results = self._search('FIND', **query)
        for result in results:
            yield result

    @command
    def search(self, search=None, facets=None, terms=None, partials=None, offset=None, limit=None, order_by=None):
        query = search_parser(search)
        if facets is not None:
            query['facets'] = facets
        if terms is not None:
            query['terms'] = terms
        if partials is not None:
            query['partials'] = partials
        if offset is not None:
            query['first'] = offset
        if limit is not None:
            query['maxitems'] = limit
        if order_by is not None:
            query['sort_by'] = order_by
        results = self._search('SEARCH', **query)
        for result in results:
            yield result

    @command
    def count(self, search=None, terms=None, partials=None):
        if search or terms or partials:
            query = search_parser(search)
            del query['facets']
            if terms is not None:
                query['terms'] = terms
            if partials is not None:
                query['partials'] = partials
            del query['first']
            query['maxitems'] = 0
            del query['sort_by']
            search = json.dumps(query, ensure_ascii=False)
        response = self._response(self.execute_command('COUNT', search))
        return int(response.split()[0])

    @command
    def delete(self, id):
        return self._response(self.execute_command('DELETE', id))

    @command
    def cdelete(self, id):
        return self._response(self.execute_command('CDELETE', id))

    def _index(self, cmd, obj, **kwargs):
        return self._response(self.execute_command(cmd, json.dumps(obj or kwargs, ensure_ascii=False)))

    @command
    def index(self, obj=None, **kwargs):
        return self._index('INDEX', obj, **kwargs)

    @command
    def cindex(self, obj=None, **kwargs):
        return self._index('CINDEX', obj, **kwargs)

    @command
    def commit(self):
        return self._response(self.execute_command('COMMIT'))


class Xapian(ServerPool):
    connection_class = XapianConnection

    def __init__(self, *args, **kwargs):
        using = kwargs.pop('using', None)
        super(Xapian, self).__init__(*args, **kwargs)
        if using:
            self.using(using)
