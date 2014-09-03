from __future__ import unicode_literals, absolute_import

import os
import sys
import time
import signal
import Queue
from hashlib import md5

import threading
import multiprocessing

# from .redis import RedisQueue as PQueue
from .fqueue import FileQueue as PQueue

import gevent
from gevent import queue

from .. import version
from ..core import xapian_database, xapian_commit, xapian_index, xapian_delete
from ..platforms import create_pidlock
from ..utils import colored_logging
from ..parser import index_parser, search_parser
from ..search import Search

from .base import CommandReceiver, CommandServer, command

import logging
colored_logging(logging)
logger = logging.getLogger(__name__)


LOG_FORMAT = "[%(asctime)s: %(levelname)s/%(processName)s:%(threadName)s] %(message)s"

STOPPED = 0
COMMIT_TIMEOUT = 1

ENDPOINTS = 'Xapian-Endpoints.db'
QUEUE_WORKER_MAIN = 'Xapian-Worker'
QUEUE_WORKER_THREAD = 'Xapian-%s'

g_tl = threading.local()
main_queue = queue.Queue()
queues = {}


class Obj(object):
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


class XapianReceiver(CommandReceiver):
    welcome = "# Welcome to Xapiand! Type quit to exit."

    def __init__(self, *args, **kwargs):
        data = kwargs.pop('data', '.')
        super(XapianReceiver, self).__init__(*args, **kwargs)
        self.data = data
        self.endpoints = None
        self.database = None
        self._do_reopen = False

    def dispatch(self, func, line):
        if getattr(func, 'db', False) and not self.endpoints:
            self.sendLine(">> ERR: %s" % "You must connect to a database first")
            return
        if getattr(func, 'reopen', False) and self._do_reopen:
            self._reopen()
        super(XapianReceiver, self).dispatch(func, line)

    @command
    def version(self, line):
        """
        Returns the version of the Xapiand server.

        Usage: VERSION

        """
        self.sendLine(">> OK: %s" % version)
    ver = version

    def _reopen(self):
        _xapian_init(self.endpoints, queue=main_queue, data=self.data, log=self.log)
        self.database = xapian_database(self.server.databases_pool, self.endpoints, False, data=self.data, log=self.log)
        self._do_reopen = False

    @command(db=True)
    def reopen(self, line=''):
        """
        Re-open the endpoint(s).

        This re-opens the endpoint(s) to the latest available version(s). It
        can be used either to make sure the latest results are returned.

        Usage: REOPEN

        """
        self._reopen()
        self.sendLine(">> OK")

    @command
    def using(self, line=''):
        """
        Start using the specified endpoint(s).

        Local paths as well as remote databases are allowed as endpoints.
        More than one endpoint can be specified, separated by spaces.

        Usage: USING <endpoint> [endpoint ...]

        """
        endpoints = tuple(line.split())
        if endpoints:
            self.endpoints = endpoints
            self._reopen()
        if self.endpoints:
            self.sendLine(">> OK")
        else:
            self.sendLine(">> ERR: [405] Select a database with the command USING")

    def _search(self, line, get_matches, get_data, get_terms, get_size):
        search = Search(
            self.database,
            line,
            get_matches=get_matches,
            get_data=get_data,
            get_terms=get_terms,
            get_size=get_size,
            data=self.data,
            log=self.log,
        )

        start = time.time()
        for line in search.results:
            self.sendLine(line)

        self.sendLine("# DEBUG: Parsed query was: %r" % search.query)
        for warning in search.warnings:
            self.sendLine("# WARNING: %s" % warning)
        size = search.size
        self.sendLine(">> OK: %s documents found in %sms" % (size, 1000.00 * (time.time() - start)))

    @command(threaded=True, db=True, reopen=True)
    def facets(self, line):
        self._search('* FACETS %s LIMIT 0' % line, get_matches=False, get_data=False, get_terms=False, get_size=False)
    facets.__doc__ = """
    Finds and lists the facets of a query.

    Usage: FACETS <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def terms(self, line):
        self._search(line, get_matches=True, get_data=False, get_terms=True, get_size=False)
    terms.__doc__ = """
    Finds and lists the terms of the documents.

    Usage: TERMS <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def find(self, line):
        self._search(line, get_matches=True, get_data=False, get_terms=False, get_size=False)
    find.__doc__ = """
    Finds documents.

    Usage: FIND <query>
    """ + search_parser.__doc__

    @command(threaded=True, db=True, reopen=True)
    def search(self, line):
        self._search(line, get_matches=True, get_data=True, get_terms=False, get_size=False)
    search.__doc__ = """
    Search documents.

    Usage: SEARCH <query>
    """ + search_parser.__doc__

    @command(db=True, reopen=True)
    def count(self, line=''):
        start = time.time()
        if line:
            self._search(line, get_matches=False, get_data=False, get_terms=False, get_size=True)
        else:
            size = self.database.get_doccount()
            self.sendLine(">> OK: %s documents found in %sms" % (size, 1000.00 * (time.time() - start)))
    count.__doc__ = """
    Counts matching documents.

    Usage: COUNT [query]

    The query can have any or a mix of:
        SEARCH query_string
        PARTIAL <partial ...> [PARTIAL <partial ...>]...
        TERMS <term ...>
    """

    def _delete(self, line, commit):
        self._do_reopen = True
        for db in self.endpoints:
            _xapian_delete(db, line, commit=commit, data=self.data, log=self.log)
        self.sendLine(">> OK")

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

    def _index(self, line, commit):
        self._do_reopen = True
        result = index_parser(line)
        if isinstance(result, tuple):
            endpoints, document = result
            for db in endpoints or self.endpoints:
                _xapian_index(db, document, commit=commit, data=self.data, log=self.log)
            self.sendLine(">> OK")
        else:
            self.sendLine(result)

    @command(db=True)
    def index(self, line):
        self._index(line, False)
    index.__doc__ = """
    Index document.

    Usage: INDEX <json>
    """ + index_parser.__doc__

    @command(db=True)
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
        for db in self.endpoints or self.endpoints:
            _xapian_commit(db, data=self.data, log=self.log)
        self.sendLine(">> OK")


class XapianServer(CommandServer):
    receiver_class = XapianReceiver

    def __init__(self, *args, **kwargs):
        data = kwargs.pop('data', '.')
        super(XapianServer, self).__init__(*args, **kwargs)
        self.data = data
        if not hasattr(g_tl, 'databases_pool'):
            g_tl.databases_pool = {}
        self.databases_pool = g_tl.databases_pool
        address = self.address[0] or '0.0.0.0'
        port = self.address[1]
        self.log.info("Listening to %s:%s", address, port)

    def buildClient(self, client_socket, address):
        return self.receiver_class(self, client_socket, address, data=self.data, log=self.log)


def get_queue(name, log=None):
    return queues.setdefault(name, PQueue(name=name, log=log))


def _flush_queue(queue):
    msg = True
    while msg is not None:
        try:
            msg = queue.get(False)
        except Queue.Empty:
            msg = None


def _database_name(db):
    return QUEUE_WORKER_THREAD % md5(db).hexdigest()


def _database_command(databases_pool, cmd, db, args, data='.', log=None):
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
            docid = xapian_index(databases_pool, db, *args, data=data, log=log)
        elif cmd == 'CINDEX':
            docid = xapian_index(databases_pool, db, *args, commit=True, data=data, log=log)
        elif cmd == 'DELETE':
            xapian_delete(databases_pool, db, *args, data=data, log=log)
        elif cmd == 'CDELETE':
            xapian_delete(databases_pool, db, *args, commit=True, data=data, log=log)
        elif cmd == 'COMMIT':
            xapian_commit(databases_pool, db, *args, data=data, log=log)
        else:
            unknown = True
    except Exception as e:
        log.exception("%s", e)
        raise
    duration = time.time() - start
    docid = ' -> %s' % docid if docid else ''
    log.debug("Executed %s %s(%s)%s (db:%s) ~ %0.3f ms", "unknown command" if unknown else "command", cmd, arg, docid, db, duration)
    return db if cmd in ('INDEX', 'DELETE') else None  # Return db if it needs to be committed.


def _database_commit(databases_pool, to_commit, commit_lock, timeouts, force=False, data='.', log=None):
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
                    log.debug("Out of commit slots, commit delayed! (db:%s)", db)
        if do_commit:
            try:
                _database_command(databases_pool, 'COMMIT', db, (), data=data, log=log)
                del to_commit[db]
            finally:
                if locked:
                    commit_lock.release()


def _enqueue(msg, queue, data='.', log=None):
    if not STOPPED:
        try:
            queue.put(msg)
        except Queue.Full:
            log.error("Cannot send command to queue! (3)")


def _xapian_init(endpoints, queue=None, data='.', log=None):
    if not queue:
        queue = get_queue(name=QUEUE_WORKER_MAIN, log=log)
    _enqueue(('INIT', endpoints, ()), queue=queue, data=data, log=log or logger)


def _xapian_commit(db, data='.', log=None):
    name = _database_name(db)
    queue = get_queue(name=os.path.join(data, name), log=log)
    _enqueue(('COMMIT', (db,), ()), queue=queue, data=data, log=log or logger)


def _xapian_index(db, document, commit=False, data='.', log=None):
    name = _database_name(db)
    queue = get_queue(name=os.path.join(data, name), log=log)
    _enqueue(('CINDEX' if commit else 'INDEX', (db,), (document,)), queue=queue, data=data, log=log or logger)


def _xapian_delete(db, document_id, commit=False, data='.', log=None):
    queue = get_queue(name=_database_name(db), log=log)
    _enqueue(('CDELETE' if commit else 'DELETE', (db,), (document_id,)), queue=queue, data=data, log=log or logger)


def _thread_loop(databases_pool, db, tq, commit_lock, timeouts, data, log):
    global STOPPED
    name = _database_name(db)
    to_commit = {}

    log.debug("Worker thread %s started! (db:%s)", name, db)

    msg = None
    timeout = timeouts.timeout
    while not STOPPED:
        _database_commit(databases_pool, to_commit, commit_lock, timeouts, data=data, log=log)

        try:
            msg = tq.get(True, timeout)
        except Queue.Empty:
            continue
        if not msg:
            continue
        try:
            cmd, endpoints, args = msg
        except ValueError:
            log.error("Wrong command received!")
            continue

        for _db in endpoints:
            if _db != db:
                log.error("Thread received command for the wrong database!")
                continue
            needs_commit = _database_command(databases_pool, cmd, db, args, data=data, log=log)
            if needs_commit:
                now = time.time()
                if needs_commit in to_commit:
                    to_commit[needs_commit] = (to_commit[needs_commit][0], to_commit[needs_commit][1], now)
                else:
                    to_commit[needs_commit] = (now, now, now)

    _database_commit(databases_pool, to_commit, commit_lock, timeouts, force=True, data=data, log=log)
    log.debug("Worker thread %s ended! (db:%s)", name, db)


def server_run(data=None, logfile=None, pidfile=None, uid=None, gid=None, umask=0,
               working_directory=None, loglevel='', commit_slots=None, commit_timeout=None,
               port=None, **options):
    global STOPPED

    log = logger

    if pidfile:
        create_pidlock(pidfile)
    if len(log.handlers) < 1:
        formatter = logging.Formatter(LOG_FORMAT)
        if logfile:
            outfile = logging.FileHandler(logfile)
            outfile.setFormatter(formatter)
            log.addHandler(outfile)
        if not pidfile:
            console = logging.StreamHandler(sys.stderr)
            console.setFormatter(formatter)
            log.addHandler(console)
    loglevel = loglevel.upper()
    if not hasattr(logging, loglevel):
        loglevel = 'INFO'
    _loglevel = getattr(logging, loglevel)
    log.setLevel(_loglevel)
    if commit_timeout is None:
        commit_timeout = COMMIT_TIMEOUT
    timeout = min(max(int(round(commit_timeout * 0.3)), 1), 3)
    commit_slots = commit_slots or multiprocessing.cpu_count()
    mode = "with multiple threads and %s commit slots" % commit_slots
    log.info("Starting Xapiand %s [%s] (pid:%s)", mode, loglevel, os.getpid())

    commit_lock = threading.Semaphore(commit_slots)
    timeouts = Obj(
        timeout=timeout,
        commit=commit_timeout * 1.0,
        delayed=commit_timeout * 3.0,
        maximum=commit_timeout * 9.0,
    )

    if ':' not in port:
        port = ':%s' % port
    server = XapianServer(port, data=data, log=log)

    databases_pool = {}
    databases = {}
    to_commit = {}

    def _server_stop(sig=None):
        global STOPPED

        now = time.time()

        if sig == signal.SIGINT:
            log.info("Hitting Ctrl+C again will terminate all running tasks!")
        elif sig:
            log.info("Sending the signal again will terminate all running tasks! (%s)", sig)

        if sig:
            if STOPPED:
                if now - STOPPED < 1:
                    sys.exit(-1)
                    return
                if now > STOPPED + 5:
                    log.info("Forcing shutdown...")
                    server.close()
            else:
                log.info("Warm shutdown... (%d open connections)", len(server.clients))
                server.close()

        STOPPED = now
        PQueue.STOPPED = STOPPED

    gevent.signal(signal.SIGQUIT, _server_stop, signal.SIGQUIT)
    gevent.signal(signal.SIGTERM, _server_stop, signal.SIGTERM)
    gevent.signal(signal.SIGINT, _server_stop, signal.SIGINT)

    log.debug("Starting server...")
    server.start()

    pq = get_queue(name=QUEUE_WORKER_MAIN, log=log)

    # Initialize seen endpoints:
    endpoints = os.path.join(data, ENDPOINTS)
    try:
        epfile = open(endpoints, 'rt').readlines()
        log.debug("Initializing endpoints...")
    except IOError:
        epfile = None
    for db in epfile or []:
        db = db.strip()
        name = _database_name(db)
        if db not in databases:
            tq = get_queue(name=os.path.join(data, name), log=log)
            t = threading.Thread(
                target=_thread_loop,
                name=name[:14],
                args=(databases_pool, db, tq, commit_lock, timeouts, data, log))
            t.start()
            databases[db] = (t, tq)
            log.info("Endpoint added! (db:%s)", db)
    try:
        epfile = open(endpoints, 'at')
    except OSError:
        log.error("Cannot write to endpoints file '%s'!", endpoints)
        epfile = None

    msg = None
    timeout = timeouts.timeout
    while not STOPPED:
        _database_commit(databases_pool, to_commit, commit_lock, timeouts, data=data, log=log)
        try:
            msg = main_queue.get(True, timeout)
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
            name = _database_name(db)
            if db in databases:
                t, tq = databases[db]
            else:
                tq = get_queue(name=os.path.join(data, name), log=log)
                t = threading.Thread(
                    target=_thread_loop,
                    name=name[:14],
                    args=(databases_pool, db, tq, commit_lock, timeouts, data, log))
                t.start()
                databases[db] = (t, tq)
                if epfile:
                    epfile.write("%s\n" % db)
                    epfile.flush()
                log.info("Endpoint added! (db:%s)", db)
            if cmd != 'INIT':
                try:
                    tq.put((cmd, db, args))
                    log.debug("Command '%s' forwarded to %s", cmd, name)
                except Queue.Full:
                    log.error("Cannot send command to queue! (2)")

    _server_stop()

    log.debug("Waiting for server to stop...")
    gevent.wait()  # Wait for worker

    for t, tq in databases.values():
        try:
            tq.put(None)  # wake up!
        except Queue.Full:
            log.error("Cannot send command to queue! (1)")

    _database_commit(databases_pool, to_commit, commit_lock, timeouts, force=True, data=data, log=log)

    log.debug("Worker joining %s threads...", len(databases))
    for t, tq in databases.values():
        t.join()

    log.info("Xapian ended! (pid:%s)", os.getpid())
