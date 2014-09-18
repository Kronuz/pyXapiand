from __future__ import unicode_literals, absolute_import

import os
import time
from hashlib import md5

from .. import version, json
from ..exceptions import InvalidIndexError, XapianError
from ..core import xapian_spawn
from ..utils import parse_url, build_url, format_time
from ..parser import index_parser, search_parser
from ..search import Search

from .base import CommandReceiver, CommandServer, command

QUEUE_WRITER_THREAD = 'Writer-%s'


def database_name(db):
    return QUEUE_WRITER_THREAD % md5(db).hexdigest()


class XapiandReceiver(CommandReceiver):
    welcome = "# Welcome to Xapiand! Type QUIT to exit, HELP for help."

    def __init__(self, *args, **kwargs):
        self.data = kwargs.pop('data', '.')
        super(XapiandReceiver, self).__init__(*args, **kwargs)
        self._do_create = False
        self._do_reopen = False
        self._do_init = set()
        self._inited = set()
        self.active_endpoints = None

    def dispatch(self, func, line, command):
        if getattr(func, 'db', False) and not self.active_endpoints:
            self.sendLine(">> ERR: %s" % "You must connect to a database first")
            return
        if getattr(func, 'reopen', False) and self._do_reopen:
            self._reopen()
        super(XapiandReceiver, self).dispatch(func, line, command)

    def _reopen(self, endpoints=None):
        endpoints = endpoints or self.active_endpoints
        self._do_init.add(endpoints)
        self._do_reopen = True

    @command
    def version(self, line):
        """
        Returns the version of the Xapiand server.

        Usage: VERSION

        """
        self.sendLine(">> OK: %s" % version)
        return version
    ver = version

    @command(db=True)
    def reopen(self, line=''):
        """
        Re-open the endpoint(s).

        This re-opens the endpoint(s) to the latest available version(s). It
        can be used either to make sure the latest results are returned.

        Usage: REOPEN

        """
        try:
            self._reopen()
            self.sendLine(">> OK")
        except InvalidIndexError as exc:
            self.sendLine(">> ERR: REOPEN: %s" % exc)

    @command
    def create(self, line=''):
        """
        Creates a database.

        Usage: CREATE <endpoint>

        """
        endpoint = line.strip()
        if endpoint:
            endpoints = (endpoint,)
            try:
                self._do_create = True
                self._reopen(endpoints=endpoints)
                self.active_endpoints = endpoints
            except InvalidIndexError as exc:
                self.sendLine(">> ERR: CREATE: %s" % exc)
            self.sendLine(">> OK")
        else:
            self.sendLine(">> ERR: [405] You must specify a valid endpoint for the database")

    @command
    def open(self, line=''):
        """
        Open the specified endpoint(s).

        Local paths as well as remote databases are allowed as endpoints.
        More than one endpoint can be specified, separated by spaces.

        Usage: OPEN <endpoint> [endpoint ...]

        See also: CREATE, USING

        """
        endpoints = line
        if endpoints:
            endpoints = tuple(endpoints.split())
            try:
                self._do_create = False
                self._reopen(endpoints=endpoints)
                self.active_endpoints = endpoints
            except InvalidIndexError as exc:
                self.sendLine(">> ERR: OPEN: %s" % exc)
                return
        if self.active_endpoints:
            self.sendLine(">> OK")
        else:
            self.sendLine(">> ERR: [405] Select a database with the command OPEN")

    @command
    def using(self, line=''):
        """
        Start using the specified endpoint(s).

        Like OPEN, but if the database doesn't exist, it creates it.

        Usage: USING <endpoint> [endpoint ...]

        See also: OPEN

        """
        endpoints = line
        if endpoints:
            endpoints = tuple(endpoints.split())
            try:
                self._do_create = True
                self._reopen(endpoints=endpoints)
                self.active_endpoints = endpoints
            except InvalidIndexError as exc:
                self.sendLine(">> ERR: USING: %s" % exc)
                return
        if self.active_endpoints:
            self.sendLine(">> OK")
        else:
            self.sendLine(">> ERR: [405] Select a database with the command OPEN")

    def _search(self, query, get_matches, get_data, get_terms, get_size, dead, counting=False):
        try:
            reopen, self._do_reopen = self._do_reopen, False
            with self.server.databases_pool.database(self.active_endpoints, writable=False, create=self._do_create, reopen=reopen) as database:
                start = time.time()

                search = Search(
                    database,
                    query,
                    get_matches=get_matches,
                    get_data=get_data,
                    get_terms=get_terms,
                    get_size=get_size,
                    data=self.data,
                    log=self.log,
                    dead=dead)

                if counting:
                    search.get_results().next()
                    size = search.estimated
                else:
                    try:
                        for result in search.results:
                            self.sendLine(json.dumps(result, ensure_ascii=False))
                    except XapianError as exc:
                        self.sendLine(">> ERR: Unable to get results: %s" % exc)
                        return

                    query_string = str(search.query)
                    self.sendLine("# DEBUG: Parsed query was: %r" % query_string)
                    for warning in search.warnings:
                        self.sendLine("# WARNING: %s" % warning)
                    size = search.size

                self.sendLine(">> OK: %s documents found in %s" % (size, format_time(time.time() - start)))
                return size
        except InvalidIndexError as exc:
            self.sendLine(">> ERR: %s" % exc)
            return

    @command(threaded=True, db=True, reopen=True)
    def facets(self, line='', dead=False):
        query = search_parser(line)
        query['facets'] = query['facets'] or query['search']
        query['search'] = '*'
        del query['first']
        query['maxitems'] = 0
        del query['sort_by']
        return self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=False, dead=dead)
    facets.__doc__ = """
    Finds and lists the facets of a query.

    Usage: FACETS <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def terms(self, line='', dead=False):
        query = search_parser(line)
        del query['facets']
        return self._search(query, get_matches=True, get_data=False, get_terms=True, get_size=True, dead=dead)
    terms.__doc__ = """
    Finds and lists the terms of the documents.

    Usage: TERMS <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def find(self, line='', dead=False):
        query = search_parser(line)
        return self._search(query, get_matches=True, get_data=False, get_terms=False, get_size=True, dead=dead)
    find.__doc__ = """
    Finds documents.

    Usage: FIND <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def search(self, line='', dead=False):
        query = search_parser(line)
        return self._search(query, get_matches=True, get_data=True, get_terms=False, get_size=True, dead=dead)
    search.__doc__ = """
    Search documents.

    Usage: SEARCH <query>
    """ + search_parser.__doc__

    @command(db=True, reopen=True)
    def count(self, line=''):
        start = time.time()
        if line:
            query = search_parser(line)
            del query['facets']
            del query['first']
            query['maxitems'] = 0
            del query['sort_by']
            return self._search(query, get_matches=False, get_data=False, get_terms=False, get_size=True, dead=False, counting=True)  # dead is False because command it's not threaded
        try:
            reopen, self._do_reopen = self._do_reopen, False
            with self.server.databases_pool.database(self.active_endpoints, writable=False, create=self._do_create, reopen=reopen) as database:
                size = database.get_doccount()
                self.sendLine(">> OK: %s documents found in %s" % (size, format_time(time.time() - start)))
                return size
        except InvalidIndexError as exc:
            self.sendLine(">> ERR: COUNT: %s" % exc)
    count.__doc__ = """
    Counts matching documents.

    Usage: COUNT [query]

    The query can have any or a mix of:
        SEARCH query_string
        PARTIAL <partial ...> [PARTIAL <partial ...>]...
        TERMS <term ...>
    """

    def _init(self):
        while self._do_init:
            endpoints = self._do_init.pop()
            if endpoints not in self._inited:
                queue = self.server.main_queue
                queue.put(('INIT', endpoints, ()))
                self._inited.add(endpoints)

    def _delete(self, document_id, commit):
        self._reopen()
        for db in self.active_endpoints:
            db = build_url(*parse_url(db.strip()))
            name = database_name(db)
            queue_name = os.path.join(self.data, name)
            queue = self.server.get_queue(queue_name)
            queue.put(('CDELETE' if commit else 'DELETE', (db,), (document_id,)))
        self.sendLine(">> OK")
        self._init()

    @command(db=True)
    def delete(self, line):
        """
        Deletes a document.

        Usage: DELETE <id>

        """
        self._delete(line, False)

    @command(db=True)
    def cdelete(self, line):
        """
        Deletes a document and commit.

        Usage: CDELETE <id>

        """
        self._delete(line, True)

    def _index(self, line, commit, **kwargs):
        result = index_parser(line)
        if isinstance(result, tuple):
            endpoints, document = result
            if not endpoints:
                endpoints = self.active_endpoints
            self._reopen(endpoints)
            if not endpoints:
                self.sendLine(">> ERR: %s" % "You must connect to a database first")
                return
            for db in endpoints:
                db = build_url(*parse_url(db.strip()))
                name = database_name(db)
                queue_name = os.path.join(self.data, name)
                queue = self.server.get_queue(queue_name)
                queue.put(('CINDEX' if commit else 'INDEX', (db,), (document,)))
            self.sendLine(">> OK")
            self._init()
        else:
            self.sendLine(result)

    @command
    def index(self, line):
        self._index(line, False)
    index.__doc__ = """
    Index document.

    Usage: INDEX <json>
    """ + index_parser.__doc__

    @command
    def cindex(self, line):
        self._index(line, True)
    cindex.__doc__ = """
    Index document and commit.

    Usage: CINDEX <json>
    """ + index_parser.__doc__

    @command(db=True)
    def commit(self, line=''):
        """
        Commits changes to the database.

        Usage: COMMIT

        """
        self._reopen()
        for db in self.active_endpoints:
            db = build_url(*parse_url(db.strip()))
            name = database_name(db)
            queue_name = os.path.join(self.data, name)
            queue = self.server.get_queue(queue_name)
            queue.put(('COMMIT', (db,), ()), data=self.data, log=self.log)
        self.sendLine(">> OK")
        self._init()

    @command(internal=True)
    def spawn(self, line=''):
        time_, address = xapian_spawn(line, data=self.data, log=self.log)
        server = "%s %s:%s" % (time_, address[0], address[1])
        self.sendLine(">> OK: %s" % server)
        return server

    @command(db=True)
    def endpoints(self, line=''):
        endpoints = self.active_endpoints or []
        for endpoint in endpoints:
            db_info = {
                'endpoint': endpoint,
            }
            self.sendLine(json.dumps(db_info))
        self.sendLine(">> OK: %d active endpoints" % len(endpoints))

    @command(internal=True)
    def databases(self, line=''):
        now = time.time()
        lines = []
        databases = self.server.databases_pool.items()
        if databases:
            for (writable, endpoints), pool_queue in databases:
                if writable:
                    lines.append("    Writer %s, pool: %s/%s, idle: ~%s" % (database_name(endpoints[0]), len(pool_queue.used), len(pool_queue.used) + len(pool_queue.unused), format_time(now - pool_queue.time)))
                    for endpoint in endpoints:
                        lines.append("        %s" % endpoint)
            for (writable, endpoints), pool_queue in databases:
                if not writable:
                    lines.append("    Reader with %s endpoint%s, pool: %s/%s, idle: ~%s" % (len(endpoints), 's' if len(endpoints) != 1 else '', len(pool_queue.used), len(pool_queue.used) + len(pool_queue.unused), format_time(now - pool_queue.time)))
                    for endpoint in endpoints:
                        lines.append("        %s" % endpoint)
        else:
            lines.append("    No active databases.")
        size = len(databases)
        self.sendLine(">> OK: %d active databases::\n%s" % (size, "\n".join(lines)))


class XapiandServer(CommandServer):
    receiver_class = XapiandReceiver

    def __init__(self, *args, **kwargs):
        self.queues = {}
        self.databases_pool = kwargs.pop('databases_pool')
        self.main_queue = kwargs.pop('main_queue')
        self.queue_class = kwargs.pop('queue_class')
        self.data = kwargs.pop('data', '.')
        super(XapiandServer, self).__init__(*args, **kwargs)
        address = self.address[0] or '0.0.0.0'
        port = self.address[1] or 8890
        self.log.info("Xapiand Server Listening to %s:%s", address, port)

    def get_queue(self, name):
        return self.queues.setdefault(name, self.queue_class(name=name, log=self.log))

    def build_client(self, client_socket, address):
        return self.receiver_class(self, client_socket, address, data=self.data, log=self.log)
