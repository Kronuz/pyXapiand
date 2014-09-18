from __future__ import unicode_literals, absolute_import

import gevent
from gevent import queue
from gevent.lock import Semaphore
from gevent.threadpool import ThreadPool

import os
import sys
import time
import Queue
import signal
import threading
from hashlib import md5

from .. import version, json
from ..exceptions import InvalidIndexError, XapianError
from ..core import DatabasesPool, xapian_cleanup, xapian_spawn, DATABASE_MAX_LIFE
from ..platforms import create_pidlock
from ..utils import parse_url, build_url, format_time
from ..parser import index_parser, search_parser
from ..search import Search

from .base import CommandReceiver, CommandServer, command
from .logging import ColoredStreamHandler
try:
    from .queue.redis import RedisQueue
except ImportError:
    RedisQueue = None
from .queue.fqueue import FileQueue
from .queue.memory import MemoryQueue


DEFAULT_QUEUE = MemoryQueue
AVAILABLE_QUEUES = {
    'file': FileQueue,
    'redis': RedisQueue or DEFAULT_QUEUE,
    'memory': MemoryQueue,
    'persistent': MemoryQueue,
    'default': DEFAULT_QUEUE,
}

import logging

LOG_FORMAT = "[%(asctime)s: %(levelname)s/%(processName)s:%(threadName)s] %(message)s"

STOPPED = 0
COMMIT_SLOTS = 10
COMMIT_TIMEOUT = 1
WRITERS_POOL_SIZE = 10
COMMANDS_POOL_SIZE = 20

WRITERS_FILE = 'Xapian-Writers.db'
QUEUE_WRITER_MAIN = 'Xapian-Worker'
QUEUE_WRITER_THREAD = 'Writer-%s'

MAIN_QUEUE = queue.Queue()
QUEUES = {}
PQueue = None


class Obj(object):
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class XapiandReceiver(CommandReceiver):
    welcome = "# Welcome to Xapiand! Type QUIT to exit, HELP for help."

    def __init__(self, *args, **kwargs):
        data = kwargs.pop('data', '.')
        super(XapiandReceiver, self).__init__(*args, **kwargs)
        self._do_create = False
        self._do_reopen = False
        self._do_init = set()
        self._inited = set()
        self.databases_pool = self.server.databases_pool
        self.active_endpoints = None
        self.data = data

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
            with self.databases_pool.database(self.active_endpoints, writable=False, create=self._do_create, reopen=reopen) as database:
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
            with self.databases_pool.database(self.active_endpoints, writable=False, create=self._do_create, reopen=reopen) as database:
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
                _xapian_init(endpoints, queue=MAIN_QUEUE, data=self.data, log=self.log)
                self._inited.add(endpoints)

    def _delete(self, line, commit):
        self._reopen()
        for db in self.active_endpoints:
            _xapian_delete(db, line, commit=commit, data=self.data, log=self.log)
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
                _xapian_index(db, document, commit=commit, data=self.data, log=self.log)
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
            _xapian_commit(db, data=self.data, log=self.log)
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
                    lines.append("    Writer %s, pool: %s/%s, idle: ~%s" % (_database_name(endpoints[0]), len(pool_queue.used), len(pool_queue.used) + len(pool_queue.unused), format_time(now - pool_queue.time)))
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
    pool_size = COMMANDS_POOL_SIZE
    receiver_class = XapiandReceiver

    def __init__(self, *args, **kwargs):
        self.data = kwargs.pop('data', '.')
        self.databases_pool = kwargs.pop('databases_pool')
        super(XapiandServer, self).__init__(*args, **kwargs)
        address = self.address[0] or '0.0.0.0'
        port = self.address[1] or 8890
        self.log.info("Xapiand Server Listening to %s:%s", address, port)

    def buildClient(self, client_socket, address):
        return self.receiver_class(self, client_socket, address, data=self.data, log=self.log)


def get_queue(name, log=logging):
    return QUEUES.setdefault(name, PQueue(name=name, log=log))


def _database_name(db):
    return QUEUE_WRITER_THREAD % md5(db).hexdigest()


def _enqueue(msg, queue, data='.', log=logging):
    if not STOPPED:
        try:
            queue.put(msg)
        except Queue.Full:
            log.error("Cannot send command to queue! (3)")


def _xapian_init(endpoints, queue=None, data='.', log=logging):
    if not queue:
        queue = get_queue(name=QUEUE_WRITER_MAIN, log=log)
    _enqueue(('INIT', endpoints, ()), queue=queue, data=data, log=log)


def _xapian_commit(db, data='.', log=logging):
    db = build_url(*parse_url(db.strip()))
    name = _database_name(db)
    queue = get_queue(name=os.path.join(data, name), log=log)
    _enqueue(('COMMIT', (db,), ()), queue=queue, data=data, log=log)


def _xapian_index(db, document, commit=False, data='.', log=logging):
    db = build_url(*parse_url(db.strip()))
    name = _database_name(db)
    queue = get_queue(name=os.path.join(data, name), log=log)
    _enqueue(('CINDEX' if commit else 'INDEX', (db,), (document,)), queue=queue, data=data, log=log)


def _xapian_delete(db, document_id, commit=False, data='.', log=logging):
    db = build_url(*parse_url(db.strip()))
    name = _database_name(db)
    queue = get_queue(name=name, log=log)
    _enqueue(('CDELETE' if commit else 'DELETE', (db,), (document_id,)), queue=queue, data=data, log=log)


def _database_command(database, cmd, args, data='.', log=logging):
    unknown = False
    start = time.time()
    if cmd in ('INDEX', 'CINDEX'):
        arg = args[0][0]
    elif cmd in ('DELETE', 'CDELETE'):
        arg = args[0]
    else:
        arg = ''
    docid = None
    try:
        if cmd == 'INDEX':
            docid = database.index(*args)
        elif cmd == 'CINDEX':
            docid = database.index(*args, commit=True)
        elif cmd == 'DELETE':
            database.delete(database, *args)
        elif cmd == 'CDELETE':
            database.delete(database, *args, commit=True)
        elif cmd == 'COMMIT':
            database.commit(*args)
        else:
            unknown = True
    except Exception as exc:
        log.exception("%s", exc)
        raise
    duration = time.time() - start
    docid = ' -> %s' % docid if docid else ''
    log.debug("Executed %s %s(%s)%s (%s) ~%s", "unknown command" if unknown else "command", cmd, arg, docid, database, format_time(duration))


def _database_commit(database, to_commit, commit_lock, timeouts, force=False, data='.', log=logging):
    if not to_commit:
        return

    now = time.time()

    expires = now - timeouts.commit
    expires_delayed = now - timeouts.delayed
    expires_max = now - timeouts.maximum

    for db, (dt0, dt1, dt2) in list(to_commit.items()):
        do_commit = locked = force and commit_lock.acquire()  # If forcing, wait for the lock
        if not do_commit:
            do_commit = dt0 <= expires_max
            if do_commit:
                log.warning("Commit maximum expiration reached, commit forced! (%s)", db)
        if not do_commit:
            if dt1 <= expires_delayed or dt2 <= expires:
                do_commit = locked = commit_lock.acquire(False)
                if not locked:
                    log.warning("Out of commit slots, commit delayed! (%s)", db)
        if do_commit:
            try:
                _database_command(database, 'COMMIT', (), data=data, log=log)
                del to_commit[db]
            finally:
                if locked:
                    commit_lock.release()


def _writer_loop(databases, databases_pool, db, tq, commit_lock, timeouts, data, log):
    global STOPPED
    name = _database_name(db)
    to_commit = {}

    current_thread = threading.current_thread()
    tid = current_thread.name.rsplit('-', 1)[-1]
    current_thread.name = '%s-%s' % (name[:14], tid)

    start = last = time.time()

    # Create a gevent Queue for this thread from the other tread's Queue
    # (using the raw underlying deque, 'queue'):
    queue = type(tq)(tq.maxsize)
    queue.queue = tq.queue

    # Open the database
    with databases_pool.database((db,), writable=True, create=True) as database:
        log.info("New writer %s: %s (%s)", name, db, database.get_uuid())
        msg = None
        timeout = timeouts.timeout
        while not STOPPED:
            _database_commit(database, to_commit, commit_lock, timeouts, data=data, log=log)

            now = time.time()
            try:
                msg = queue.get(True, timeout)
            except Queue.Empty:
                if now - last > DATABASE_MAX_LIFE:
                    log.debug("Writer timeout... stopping!")
                    break
                continue
            if not msg:
                continue
            try:
                cmd, endpoints, args = msg
            except ValueError:
                log.error("Wrong command received!")
                continue

            for _db in endpoints:
                _db = build_url(*parse_url(_db.strip()))
                if _db != db:
                    continue

                last = now
                _database_command(database, cmd, args, data=data, log=log)

                if cmd in ('INDEX', 'DELETE'):
                    now = time.time()
                    if db in to_commit:
                        to_commit[db] = (to_commit[db][0], to_commit[db][1], now)
                    else:
                        to_commit[db] = (now, now, now)

        _database_commit(database, to_commit, commit_lock, timeouts, force=True, data=data, log=log)
        database.close()
        databases.pop(db, None)

    log.debug("Writer %s ended! ~ lived for %s", name, format_time(time.time() - start))


def xapiand_run(data=None, logfile=None, pidfile=None, uid=None, gid=None, umask=0,
        working_directory=None, verbosity=2, commit_slots=None, commit_timeout=None,
        port=None, queue=None, **options):
    global PQueue, STOPPED

    current_thread = threading.current_thread()
    tid = current_thread.name.rsplit('-', 1)[-1]
    current_thread.name = 'Server-%s' % tid

    if pidfile:
        create_pidlock(pidfile)

    address, _, port = port.partition(':')
    if not port:
        port, address = address, ''
    port = int(port)

    loglevel = ['ERROR', 'WARNING', 'INFO', 'DEBUG'][3 if verbosity == 'v' else int(verbosity)]

    log = logging.getLogger()
    log.setLevel(loglevel)
    if len(log.handlers) < 1:
        formatter = logging.Formatter(LOG_FORMAT)
        if logfile:
            outfile = logging.FileHandler(logfile)
            outfile.setFormatter(formatter)
            log.addHandler(outfile)
        if not pidfile:
            console = ColoredStreamHandler(sys.stderr)
            console.setFormatter(formatter)
            log.addHandler(console)

    if not commit_slots:
        commit_slots = COMMIT_SLOTS

    if commit_timeout is None:
        commit_timeout = COMMIT_TIMEOUT
    timeout = min(max(int(round(commit_timeout * 0.3)), 1), 3)

    PQueue = AVAILABLE_QUEUES.get(queue) or AVAILABLE_QUEUES['default']
    mode = "with multiple threads and %s commit slots using %s" % (commit_slots, PQueue.__name__)
    log.warning("Starting Xapiand Server v%s %s [%s] (pid:%s)", version, mode, loglevel, os.getpid())

    commit_lock = Semaphore(commit_slots)
    timeouts = Obj(
        timeout=timeout,
        commit=commit_timeout * 1.0,
        delayed=commit_timeout * 3.0,
        maximum=commit_timeout * 9.0,
    )

    databases_pool = DatabasesPool(data=data, log=log)
    databases = {}

    xapian_server = XapiandServer((address, port), databases_pool=databases_pool, data=data, log=log)

    gevent.signal(signal.SIGTERM, xapian_server.close)
    gevent.signal(signal.SIGINT, xapian_server.close)

    log.debug("Starting server...")
    try:
        xapian_server.start()
    except Exception as exc:
        log.error("Cannot start server: %s", exc)
        sys.exit(-1)

    pq = get_queue(name=QUEUE_WRITER_MAIN, log=log)

    pool_size = WRITERS_POOL_SIZE
    pool_size_warning = int(pool_size / 3.0 * 2.0)
    writers_pool = ThreadPool(pool_size)

    def start_writer(db):
        db = build_url(*parse_url(db.strip()))
        name = _database_name(db)
        try:
            tq = None
            t, tq = databases[db]
            if t.ready():
                raise KeyError
        except KeyError:
            tq = tq or get_queue(name=os.path.join(data, name), log=log)
            pool_used = len(writers_pool)
            if not (pool_size_warning - pool_used) % 10:
                log.warning("Writers pool is close to be full (%s/%s)", pool_used, pool_size)
            elif pool_used == pool_size:
                log.error("Writers poll is full! (%s/%s)", pool_used, pool_size)
            t = writers_pool.spawn(_writer_loop, databases, databases_pool, db, tq, commit_lock, timeouts, data, log)
            databases[db] = (t, tq)
        return db, name, t, tq

    if PQueue.persistent:
        # Initialize seen writers:
        writers_file = os.path.join(data, WRITERS_FILE)
        with open(writers_file, 'rt') as epfile:
            for i, db in enumerate(epfile):
                if i == 0:
                    log.debug("Initializing writers...")
                start_writer(db)

    log.info("Waiting for commands...")
    msg = None
    timeout = timeouts.timeout
    while not xapian_server.closed:
        xapian_cleanup(databases_pool, DATABASE_MAX_LIFE, data=data, log=log)
        try:
            msg = MAIN_QUEUE.get(True, timeout)
        except Queue.Empty:
            try:
                msg = pq.get(False)
            except Queue.Empty:
                continue
        if not msg:
            continue
        try:
            cmd, endpoints, args = msg
        except ValueError:
            log.error("Wrong command received!")
            continue

        for db in endpoints:
            db, name, t, tq = start_writer(db)
            if cmd != 'INIT':
                try:
                    tq.put((cmd, db, args))
                    log.debug("Command '%s' forwarded to %s", cmd, name)
                except Queue.Full:
                    log.error("Cannot send command to queue! (2)")

    log.debug("Waiting for connected clients to disconnect...")
    while True:
        if xapian_server.close(max_age=10):
            break
        if gevent.wait(timeout=3):
            break

    # Stop queues:
    PQueue.STOPPED = STOPPED = time.time()
    if PQueue.persistent:
        with open(writers_file, 'wt') as epfile:
            for db, (t, tq) in databases.items():
                if not t.ready():
                    epfile.write("%s\n" % db)

    # Wake up writers:
    for t, tq in databases.values():
        try:
            tq.put(None)  # wake up!
        except Queue.Full:
            log.error("Cannot send command to queue! (1)")

    log.debug("Waiting for %s writers...", len(databases))
    for t, tq in databases.values():
        t.wait()

    xapian_cleanup(databases_pool, 0, data=data, log=log)

    log.warning("Xapiand Server ended! (pid:%s)", os.getpid())

    gevent.wait()
