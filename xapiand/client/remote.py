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
        response = self._response(self.execute_command('CRETE', endpoint))
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

    def _search(self, cmd, query):
        line = self.execute_command(cmd, query)
        while line:
            response = self._response(line)
            if response:
                break
            yield json.loads(line)
            line = self.read()

    @command
    def facets(self, query):
        for r in self._search('FACETS', query):
            yield r

    @command
    def terms(self, query):
        for r in self._search('TERMS', query):
            yield r

    @command
    def find(self, query):
        for r in self._search('FIND', query):
            yield r

    @command
    def search(self, query):
        for r in self._search('SEARCH', query):
            yield r

    @command
    def count(self, query=''):
        response = self._response(self.execute_command('COUNT', query))
        return int(response.split()[0])

    @command
    def delete(self, document_id):
        return self._response(self.execute_command('DELETE', document_id))

    @command
    def cdelete(self, document_id):
        return self._response(self.execute_command('CDELETE', document_id))

    @command
    def index(self, obj):
        return self._response(self.execute_command('INDEX', json.dumps(obj, ensure_ascii=False)))

    @command
    def cindex(self, obj):
        return self._response(self.execute_command('CINDEX', json.dumps(obj, ensure_ascii=False)))

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
