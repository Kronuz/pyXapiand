from __future__ import unicode_literals, absolute_import

import os
import sys
import time
import signal
import Queue
from hashlib import md5

import threading
import multiprocessing

import gevent
from gevent import queue
from gevent.socket import create_connection

from .. import version, json
from ..exceptions import InvalidIndexError, XapianError
from ..core import DatabasesPool, xapian_database, xapian_close, xapian_commit, xapian_index, xapian_delete
from ..platforms import create_pidlock
from ..utils import parse_url, build_url, format_time
from ..parser import index_parser, search_parser
from ..search import Search

from .base import PortForwarder, CommandReceiver, CommandServer, command
from .logging import QueueHandler, ColoredStreamHandler
try:
    from .redis import RedisQueue
except ImportError:
    RedisQueue = None
from .fqueue import FileQueue
from .memory import MemoryQueue

AVAILABLE_QUEUES = {
    'file': FileQueue,
    'redis': RedisQueue or MemoryQueue,
    'memory': MemoryQueue,
    None: MemoryQueue,
}

import logging

LOG_FORMAT = "[%(asctime)s: %(levelname)s/%(processName)s:%(threadName)s] %(message)s"

STOPPED = 0
COMMIT_TIMEOUT = 1
DATABASE_MAX_LIFE = 900  # stop writer adter 15 minutes of inactivity

WRITERS_FILE = 'Xapian-Writers.db'
QUEUE_WORKER_MAIN = 'Xapian-Worker'
QUEUE_WORKER_THREAD = 'Xapian-%s'

MAIN_QUEUE = queue.Queue()
QUEUES = {}
PQueue = None


class Obj(object):
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class XapiandForwarder(PortForwarder):
    def __init__(self, *args, **kwargs):
        super(XapiandForwarder, self).__init__(*args, **kwargs)
        address = self.address[0] or '0.0.0.0'
        port = self.address[1]
        self.log.info("Xapiand Forwarder Listening to %s:%s", address, port)

    def create_connection(self):
        return create_connection(('127.0.0.0', 8901))


class XapiandReceiver(CommandReceiver):
    welcome = "# Welcome to Xapiand! Type QUIT to exit, HELP for help."

    def __init__(self, *args, **kwargs):
        data = kwargs.pop('data', '.')
        super(XapiandReceiver, self).__init__(*args, **kwargs)
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

    def _reopen(self, create=False, endpoints=None):
        endpoints = endpoints or self.active_endpoints
        with xapian_database(self.databases_pool, endpoints, writable=False, create=create, reopen=True, data=self.data, log=self.log):
            self._do_reopen = False
            self._do_init.add(endpoints)

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
        except InvalidIndexError as e:
            self.sendLine(">> ERR: Reopen: %s" % e)

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
                self._reopen(create=True, endpoints=endpoints)
                self.active_endpoints = endpoints
            except InvalidIndexError as e:
                self.sendLine(">> ERR: Create: %s" % e)
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
                self._reopen(create=False, endpoints=endpoints)
                self.active_endpoints = endpoints
            except InvalidIndexError as e:
                self.sendLine(">> ERR: Using: %s" % e)
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
                self._reopen(create=True, endpoints=endpoints)
                self.active_endpoints = endpoints
            except InvalidIndexError as e:
                self.sendLine(">> ERR: Using: %s" % e)
                return
        if self.active_endpoints:
            self.sendLine(">> OK")
        else:
            self.sendLine(">> ERR: [405] Select a database with the command OPEN")

    def _search(self, query, get_matches, get_data, get_terms, get_size, dead, counting=False):
        try:
            with xapian_database(self.databases_pool, self.active_endpoints, writable=False, data=self.data, log=self.log) as database:
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
                    except XapianError as e:
                        self.sendLine(">> ERR: Unable to get results: %s" % e)
                        return

                    query_string = str(search.query)
                    self.sendLine("# DEBUG: Parsed query was: %r" % query_string)
                    for warning in search.warnings:
                        self.sendLine("# WARNING: %s" % warning)
                    size = search.size

                self.sendLine(">> OK: %s documents found in %s" % (size, format_time(time.time() - start)))
                return size
        except InvalidIndexError as e:
            self.sendLine(">> ERR: %s" % e)
            return

    @command(threaded=True, db=True, reopen=True)
    def facets(self, line, dead):
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
    def terms(self, line, dead):
        query = search_parser(line)
        del query['facets']
        return self._search(query, get_matches=True, get_data=False, get_terms=True, get_size=True, dead=dead)
    terms.__doc__ = """
    Finds and lists the terms of the documents.

    Usage: TERMS <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def find(self, line, dead):
        query = search_parser(line)
        return self._search(query, get_matches=True, get_data=False, get_terms=False, get_size=True, dead=dead)
    find.__doc__ = """
    Finds documents.

    Usage: FIND <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def search(self, line, dead):
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
            with xapian_database(self.databases_pool, self.active_endpoints, writable=False, data=self.data, log=self.log) as database:
                size = database.get_doccount()
                self.sendLine(">> OK: %s documents found in %s" % (size, format_time(time.time() - start)))
                return size
        except InvalidIndexError as e:
            self.sendLine(">> ERR: Count: %s" % e)
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
        self._do_reopen = True
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
        self._do_reopen = True
        result = index_parser(line)
        if isinstance(result, tuple):
            endpoints, document = result
            if not endpoints:
                endpoints = self.active_endpoints
            else:
                self._do_init.add(endpoints)
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
        self._do_reopen = True
        for db in self.active_endpoints:
            _xapian_commit(db, data=self.data, log=self.log)
        self.sendLine(">> OK")
        self._init()

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
    receiver_class = XapiandReceiver

    def __init__(self, *args, **kwargs):
        self.data = kwargs.pop('data', '.')
        self.databases_pool = kwargs.pop('databases_pool')
        super(XapiandServer, self).__init__(*args, **kwargs)
        address = self.address[0] or '0.0.0.0'
        port = self.address[1]
        self.log.info("Xapiand Server Listening to %s:%s", address, port)

    def buildClient(self, client_socket, address):
        return self.receiver_class(self, client_socket, address, data=self.data, log=self.log)


def get_queue(name, log=logging):
    return QUEUES.setdefault(name, PQueue(name=name, log=log))


def _flush_queue(queue):
    msg = True
    while msg is not None:
        try:
            msg = queue.get(False)
        except Queue.Empty:
            msg = None


def _database_name(db):
    return QUEUE_WORKER_THREAD % md5(db).hexdigest()


def _database_command(database, cmd, db, args, data='.', log=logging):
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
            docid = xapian_index(database, db, *args, data=data, log=log)
        elif cmd == 'CINDEX':
            docid = xapian_index(database, db, *args, commit=True, data=data, log=log)
        elif cmd == 'DELETE':
            xapian_delete(database, db, *args, data=data, log=log)
        elif cmd == 'CDELETE':
            xapian_delete(database, db, *args, commit=True, data=data, log=log)
        elif cmd == 'COMMIT':
            xapian_commit(database, db, *args, data=data, log=log)
        else:
            unknown = True
    except Exception as e:
        log.exception("%s", e)
        raise
    duration = time.time() - start
    docid = ' -> %s' % docid if docid else ''
    log.debug("Executed %s %s(%s)%s (%s) ~%s", "unknown command" if unknown else "command", cmd, arg, docid, db, format_time(duration))
    return db if cmd in ('INDEX', 'DELETE') else None  # Return db if it needs to be committed.


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
        if not do_commit:
            if dt1 <= expires_delayed or dt2 <= expires:
                do_commit = locked = commit_lock.acquire(False)
                if not locked:
                    log.debug("Out of commit slots, commit delayed! (%s)", db)
        if do_commit:
            try:
                _database_command(database, 'COMMIT', db, (), data=data, log=log)
                del to_commit[db]
            finally:
                if locked:
                    commit_lock.release()


def _enqueue(msg, queue, data='.', log=logging):
    if not STOPPED:
        try:
            queue.put(msg)
        except Queue.Full:
            log.error("Cannot send command to queue! (3)")


def _xapian_init(endpoints, queue=None, data='.', log=logging):
    if not queue:
        queue = get_queue(name=QUEUE_WORKER_MAIN, log=log)
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


def _writer_loop(databases, databases_pool, db, tq, commit_lock, timeouts, data, log):
    global STOPPED
    name = _database_name(db)
    to_commit = {}

    start = last = time.time()
    log.debug("New writer %s: %s", name, db)

    # Open the database
    with xapian_database(databases_pool, (db,), writable=True, create=True, data=data, log=log) as database:
        msg = None
        timeout = timeouts.timeout
        while not STOPPED:
            _database_commit(database, to_commit, commit_lock, timeouts, data=data, log=log)

            now = time.time()
            try:
                msg = tq.get(True, timeout)
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
                needs_commit = _database_command(database, cmd, db, args, data=data, log=log)

                if needs_commit:
                    now = time.time()
                    if needs_commit in to_commit:
                        to_commit[needs_commit] = (to_commit[needs_commit][0], to_commit[needs_commit][1], now)
                    else:
                        to_commit[needs_commit] = (now, now, now)

        _database_commit(database, to_commit, commit_lock, timeouts, force=True, data=data, log=log)
        xapian_close(database, data=data, log=log)
        databases.pop(db, None)

    log.debug("Writer %s ended! ~ lived for %s", name, format_time(time.time() - start))


def logger_run(loglevel, log_queue, logfile, pidfile):
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

    log.warning("Starting Xapiand Logger (pid:%s)", os.getpid())

    quit = False
    while True:
        try:
            record = log_queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break
            log.handle(record)  # No level or filter logic applied - just do it!
        except (KeyboardInterrupt, SystemExit):
            if quit:
                raise
            quit = True
            continue
        except:
            import traceback
            traceback.print_exc(file=sys.stderr)

    log.warning("Xapiand Logger ended! (pid:%s)", os.getpid())


def forwarder_run(loglevel, log_queue, address, port):
    log = logging.getLogger()
    log.addHandler(QueueHandler(log_queue))
    log.setLevel(loglevel)

    log.warning("Starting Xapiand Forwarder (pid:%s)", os.getpid())

    xapian_forwarder = XapiandForwarder((address, port))

    def _server_stop(sig=None):
        global STOPPED

        now = time.time()

        stopped = STOPPED

        if sig:
            if stopped:
                if now - stopped < 0.5:
                    log.error("Killing forwarder process!...")
                    sys.exit(-1)
                    return
                if now - stopped > 1:
                    log.error("Forcing forwarder shutdown...")
                    xapian_forwarder.close()
            else:
                log.info("Warm forwarder shutdown... (%d open connections)", len(xapian_forwarder.sockets))
                xapian_forwarder.close()

    # gevent.signal(signal.SIGQUIT, _server_stop, signal.SIGQUIT)
    gevent.signal(signal.SIGTERM, _server_stop, signal.SIGTERM)
    gevent.signal(signal.SIGINT, _server_stop, signal.SIGINT)

    log.debug("Starting forwarder...")
    xapian_forwarder.start()

    gevent.wait()

    _server_stop()

    log.debug("Waiting for forwarder to stop...")
    gevent.wait()  # Wait for worker

    log.warning("Xapiand Forwarder ended! (pid:%s)", os.getpid())


def server_run(loglevel, log_queue, address, port, commit_slots, commit_timeout, data):
    global PQueue, STOPPED

    log = logging.getLogger()
    log.addHandler(QueueHandler(log_queue))
    log.setLevel(loglevel)

    commit_slots = commit_slots or multiprocessing.cpu_count()

    if commit_timeout is None:
        commit_timeout = COMMIT_TIMEOUT
    timeout = min(max(int(round(commit_timeout * 0.3)), 1), 3)

    PQueue = AVAILABLE_QUEUES.get(queue) or AVAILABLE_QUEUES[None]
    mode = "with multiple threads and %s commit slots using %s" % (commit_slots, PQueue.__name__)
    log.warning("Starting Xapiand Server %s %s [%s] (pid:%s)", version, mode, loglevel, os.getpid())

    commit_lock = threading.Semaphore(commit_slots)
    timeouts = Obj(
        timeout=timeout,
        commit=commit_timeout * 1.0,
        delayed=commit_timeout * 3.0,
        maximum=commit_timeout * 9.0,
    )

    databases_pool = DatabasesPool()
    databases = {}

    xapian_server = XapiandServer((address, port), databases_pool=databases_pool, data=data, log=log)

    def _server_stop(sig=None):
        global STOPPED

        now = time.time()

        if sig == signal.SIGINT:
            log.warning("Hitting Ctrl+C again will terminate all running tasks!")
        elif sig:
            log.warning("Sending the signal again will terminate all running tasks! (%s)", sig)

        stopped = STOPPED
        if not stopped:
            PQueue.STOPPED = STOPPED = now

        if sig:
            if stopped:
                if now - stopped < 0.5:
                    log.error("Killing server process!...")
                    sys.exit(-1)
                    return
                if now - stopped > 1:
                    log.error("Forcing server shutdown...")
                    xapian_server.close()
            else:
                log.info("Warm server shutdown... (%d open connections)", len(xapian_server.clients))
                xapian_server.close()

    # gevent.signal(signal.SIGQUIT, _server_stop, signal.SIGQUIT)
    gevent.signal(signal.SIGTERM, _server_stop, signal.SIGTERM)
    gevent.signal(signal.SIGINT, _server_stop, signal.SIGINT)

    log.debug("Starting server...")
    xapian_server.start()

    pq = get_queue(name=QUEUE_WORKER_MAIN, log=log)

    def start_writer(db):
        db = build_url(*parse_url(db.strip()))
        name = _database_name(db)
        try:
            tq = None
            t, tq = databases[db]
            if not t.is_alive():
                raise KeyError
        except KeyError:
            tq = tq or get_queue(name=os.path.join(data, name), log=log)
            t = threading.Thread(
                target=_writer_loop,
                name=name[:14],
                args=(databases, databases_pool, db, tq, commit_lock, timeouts, data, log))
            databases[db] = (t, tq)
            t.start()
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
    while not STOPPED:
        databases_pool.cleanup(DATABASE_MAX_LIFE, data=data, log=log)
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

    _server_stop()

    log.debug("Waiting for server to stop...")
    gevent.wait()  # Wait for worker

    if PQueue.persistent:
        with open(writers_file, 'wt') as epfile:
            for db, (t, tq) in databases.items():
                if t.is_alive():
                    epfile.write("%s\n" % db)

    # Wake up writers:
    for t, tq in databases.values():
        try:
            tq.put(None)  # wake up!
        except Queue.Full:
            log.error("Cannot send command to queue! (1)")

    log.debug("Worker joining %s threads...", len(databases))
    for t, tq in databases.values():
        t.join()

    log.warning("Xapiand Server ended! (pid:%s)", os.getpid())


def xapiand_run(data=None, logfile=None, pidfile=None, uid=None, gid=None, umask=0,
        working_directory=None, verbosity=2, commit_slots=None, commit_timeout=None,
        port=None, queue=None, **options):
    logger_job = forwarder_job = server_job = None

    if pidfile:
        create_pidlock(pidfile)

    address, _, port = port.partition(':')
    if not port:
        port, address = address, ''
    port = int(port)

    loglevel = ['ERROR', 'WARNING', 'INFO', 'DEBUG'][3 if verbosity == 'v' else int(verbosity)]
    log_queue = multiprocessing.Queue()

    logger_job = multiprocessing.Process(
        name="Xapiand-Logger",
        target=logger_run,
        args=(loglevel, log_queue, logfile, pidfile),
    )
    logger_job.start()

    # forwarder_job = multiprocessing.Process(
    #     name="Xapiand-Forwarder",
    #     target=forwarder_run,
    #     args=(loglevel, log_queue, address, port + 1),
    # )
    # forwarder_job.start()

    server_job = multiprocessing.Process(
        name="Xapiand-Server",
        target=server_run,
        args=(loglevel, log_queue, address, port, commit_slots, commit_timeout, data),
    )
    server_job.start()

    quit = False
    while True:
        try:
            if forwarder_job:
                forwarder_job.join()

            if server_job:
                server_job.join()

            log_queue.put_nowait(None)
            logger_job.join()

            break
        except (KeyboardInterrupt, SystemExit):
            if quit:
                if forwarder_job:
                    forwarder_job.terminate()

                if server_job:
                    server_job.terminate()

                logger_job.terminate()
                raise
            quit = True
            continue
        except:
            import traceback
            traceback.print_exc(file=sys.stderr)
