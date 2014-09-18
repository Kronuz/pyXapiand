from __future__ import unicode_literals, absolute_import

import os
import sys
import time
import Queue
import signal
import threading

import gevent
from gevent import queue
from gevent.lock import Semaphore
from gevent.threadpool import ThreadPool

from .. import version
from ..core import DatabasesPool, xapian_cleanup, DATABASE_MAX_LIFE
from ..utils import parse_url, build_url, format_time
from ..platforms import create_pidlock

from .logging import ColoredStreamHandler
from .server import XapiandServer, database_name

try:
    from .queue.redis import RedisQueue
except ImportError:
    RedisQueue = None
from .queue.fqueue import FileQueue
from .queue.memory import MemoryQueue


STOPPED = 0
COMMIT_SLOTS = 10
COMMIT_TIMEOUT = 1
WRITERS_POOL_SIZE = 10
COMMANDS_POOL_SIZE = 20

WRITERS_FILE = 'Xapian-Writers.db'

LOG_FORMAT = "[%(asctime)s: %(levelname)s/%(processName)s:%(threadName)s] %(message)s"

DEFAULT_QUEUE = MemoryQueue
AVAILABLE_QUEUES = {
    'file': FileQueue,
    'redis': RedisQueue or DEFAULT_QUEUE,
    'memory': MemoryQueue,
    'persistent': MemoryQueue,
    'default': DEFAULT_QUEUE,
}

import logging


class Obj(object):
    def __init__(self, **kwargs):
        self.__dict__ = kwargs


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
    name = database_name(db)
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
        listener=None, queue_type=None, **options):
    global STOPPED

    current_thread = threading.current_thread()
    tid = current_thread.name.rsplit('-', 1)[-1]
    current_thread.name = 'Server-%s' % tid

    if pidfile:
        create_pidlock(pidfile)

    address, _, port = listener.partition(':')
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

    queue_class = AVAILABLE_QUEUES.get(queue_type) or AVAILABLE_QUEUES['default']
    mode = "with multiple threads and %s commit slots using %s" % (commit_slots, queue_class.__name__)
    log.warning("Starting Xapiand Server v%s %s [%s] (pid:%s)", version, mode, loglevel, os.getpid())

    commit_lock = Semaphore(commit_slots)
    timeouts = Obj(
        timeout=timeout,
        commit=commit_timeout * 1.0,
        delayed=commit_timeout * 3.0,
        maximum=commit_timeout * 9.0,
    )

    main_queue = queue.Queue()
    databases_pool = DatabasesPool(data=data, log=log)
    databases = {}

    xapian_server = XapiandServer(
        (address, port),
        databases_pool=databases_pool,
        pool_size=COMMANDS_POOL_SIZE,
        main_queue=main_queue,
        queue_class=queue_class,
        data=data,
        log=log
    )

    gevent.signal(signal.SIGTERM, xapian_server.close)
    gevent.signal(signal.SIGINT, xapian_server.close)

    log.debug("Starting server...")
    try:
        xapian_server.start()
    except Exception as exc:
        log.error("Cannot start server: %s", exc)
        sys.exit(-1)

    pool_size = WRITERS_POOL_SIZE
    pool_size_warning = int(pool_size / 3.0 * 2.0)
    writers_pool = ThreadPool(pool_size)

    def start_writer(db):
        db = build_url(*parse_url(db.strip()))
        name = database_name(db)
        try:
            tq = None
            t, tq = databases[db]
            if t.ready():
                raise KeyError
        except KeyError:
            queue_name = os.path.join(data, name)
            tq = tq or xapian_server.get_queue(queue_name)
            pool_used = len(writers_pool)
            if not (pool_size_warning - pool_used) % 10:
                log.warning("Writers pool is close to be full (%s/%s)", pool_used, pool_size)
            elif pool_used == pool_size:
                log.error("Writers poll is full! (%s/%s)", pool_used, pool_size)
            t = writers_pool.spawn(_writer_loop, databases, databases_pool, db, tq, commit_lock, timeouts, data, log)
            databases[db] = (t, tq)
        return db, name, t, tq

    if queue_class.persistent:
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
            msg = main_queue.get(True, timeout)
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
    queue_class.STOPPED = STOPPED = time.time()
    if queue_class.persistent:
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
