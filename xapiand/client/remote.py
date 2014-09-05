from __future__ import absolute_import, unicode_literals

from .. import json

from ..exceptions import XapianError
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

    def _search(self, cmd, query_string, **kwargs):
        line = self.execute_command(cmd, json.dumps(query_string or kwargs, ensure_ascii=False))
        while line:
            response = self._response(line)
            if response:
                break
            yield json.loads(line)
            line = self.read()

    @command
    def facets(self, query_string=None, partial=None, terms=None, search=None):
        for r in self._search('FACETS', query_string, partial=partial, terms=terms, search=search):
            yield r

    @command
    def terms(self, query_string=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        for r in self._search('TERMS', query_string, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by):
            yield r

    @command
    def find(self, query_string=None, facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        for r in self._search('FIND', query_string, facets=facets, terms=terms, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by):
            yield r

    @command
    def search(self, query_string=None, facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        for r in self._search('SEARCH', query_string, facets=facets, terms=terms, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by):
            yield r

    @command
    def count(self, query_string=None, partial=None, search=None):
        response = self._response(self.execute_command('COUNT', query_string))
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
