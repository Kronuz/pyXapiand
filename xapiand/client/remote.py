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
        line = line.decode(self.encoding)
        if line.startswith(">>"):
            if line.startswith(">> OK"):
                return line[7:] or None
            if line.startswith(">> ERR"):
                raise XapianError(line[8:])

    def _search(self, cmd, query, facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
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

        line = self.execute_command(cmd, query)
        while line:
            response = self._response(line)
            if response:
                break
            yield json.loads(line)
            line = self.read()

    @command
    def facets(self, query='', partial=None, terms=None, search=None):
        for r in self._search('FACETS', query, partial=partial, terms=terms, search=search):
            yield r

    @command
    def terms(self, query='', partial=None, search=None, offset=None, limit=None, order_by=None):
        for r in self._search('TERMS', query, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by):
            yield r

    @command
    def find(self, query='', facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        for r in self._search('FIND', query, facets=facets, terms=terms, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by):
            yield r

    @command
    def search(self, query='', facets=None, terms=None, partial=None, search=None, offset=None, limit=None, order_by=None):
        for r in self._search('SEARCH', query, facets=facets, terms=terms, partial=partial, search=search, offset=offset, limit=limit, order_by=order_by):
            yield r

    @command
    def count(self, query=''):
        response = self._response(self.execute_command('COUNT', query))
        return int(response.split()[0])

    @command
    def delete(self, id):
        return self._response(self.execute_command('DELETE', id))

    @command
    def cdelete(self, id):
        return self._response(self.execute_command('CDELETE', id))

    @command
    def index(self, obj=None, **kwargs):
        return self._response(self.execute_command('INDEX', json.dumps(obj or kwargs, ensure_ascii=False)))

    @command
    def cindex(self, obj=None, **kwargs):
        return self._response(self.execute_command('CINDEX', json.dumps(obj or kwargs, ensure_ascii=False)))

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
