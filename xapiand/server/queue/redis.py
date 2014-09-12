from __future__ import absolute_import, unicode_literals

import time
from cPickle import loads, dumps

import threading
import Queue

import redis
from redis.exceptions import ConnectionError

from contextlib import contextmanager

import logging

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 6379
DEFAULT_DB = 0


class VersionMismatch(Exception):
    pass


class RedisQueue(object):
    persistent = True

    STOPPED = False
    CONNECTED = False
    retry = 0
    _retry = 0
    _client = None
    _lock = threading.RLock()
    socket_timeout = None

    def __init__(self, name=None, log=logging):
        if redis.VERSION < (2, 4, 4):
            raise VersionMismatch(
                'Redis transport requires redis-py versions 2.4.4 or later. '
                'You have {0.__version__}'.format(redis))

        self.names = ()
        self.add(name)
        self.logger = log

    def add(self, name):
        if not hasattr(name, '__iter__'):
            name = [name]
        self.names += tuple(name)
        self.keys = tuple('_queue_%s' % n for n in self.names)
        self.hostname = DEFAULT_HOST
        self.port = DEFAULT_PORT
        self.database = DEFAULT_DB
        self.password = None

    def _connparams(self):
        return {'host': self.hostname,
                'port': self.port,
                'db': self.database,
                'password': self.password,
                'socket_timeout': self.socket_timeout}

    def _create_client(self):
        return redis.Redis(**self._connparams())

    @property
    def client(self):
        if self._client is None:
            self._client = self._create_client()
        return self._client

    @contextmanager
    def conn_or_acquire(self, client=None, retry=True):
        if not client:
            client = self.client

        def _conn_or_acquire(stop, retry, try_connecting):
            self.__class__._lock.acquire()
            # print 'lock acquired!!', "(pid:%s, tid:%s)" % (os.getpid(), threading.current_thread().ident)
            try:
                if self.__class__.STOPPED:
                    if not stop:
                        return None

                if retry:
                    will_retry = self.retry
                    if will_retry < 32:
                        will_retry += 2
                    elif will_retry == 32:
                        will_retry = 33
                    else:
                        will_retry = 32
                    # print 'retry:', will_retry, self.__class__._retry, "(pid:%s, tid:%s)" % (os.getpid(), threading.current_thread().ident)
                    if will_retry > self.__class__._retry:
                        global_retry = will_retry
                        try_connecting = True
                    else:
                        global_retry = will_retry = self.__class__._retry
                        try_connecting = False

                if try_connecting:
                    try:
                        # print 'client.info()', "(pid:%s, tid:%s)" % (os.getpid(), threading.current_thread().ident)
                        client.info()
                        self.__class__.CONNECTED = True
                        if self.__class__._retry != 0:
                            self.__class__._retry = 0
                            self.retry = 0
                            self.logger.info("Connected to redis://%s:%s/%s/", self.hostname, self.port, self.database)
                        stop = True
                        return client
                    except ConnectionError as e:
                        self.__class__.CONNECTED = False
                        if retry:
                            self.retry = will_retry
                            self.__class__._retry = global_retry
                            self.logger.error("Cannot connect to redis://%s:%s/%s/: %s. Trying again in %0.2f seconds...", self.hostname, self.port, self.database, e, will_retry)
                        else:
                            if not stop:
                                return None
                            raise StopIteration
                elif not retry:
                    if not stop:
                        return None
                    raise StopIteration
            finally:
                # print 'lock released!!', "(pid:%s, tid:%s)" % (os.getpid(), threading.current_thread().ident)
                self.__class__._lock.release()

            for i in range(will_retry):
                if self.__class__.STOPPED or self.__class__.CONNECTED:
                    break
                # print 'sleep! (%s)' % i, "(pid:%s, tid:%s)" % (os.getpid(), threading.current_thread().ident)
                time.sleep(1)
            return _conn_or_acquire(stop, False, False)

        yield _conn_or_acquire(False, retry, True)

    def get(self, block, timeout=None):
        item = None
        try:
            with self.conn_or_acquire() as client:
                if client:
                    if block and (timeout or timeout is None):
                        item = client.brpop(self.keys, timeout or 0)
                        if item:
                            item = item[1]
                    else:
                        item = client.rpop(self.keys)
        except ConnectionError:
            item = None
        if item is None:
            raise Queue.Empty
        return loads(item)

    def put(self, value):
        try:
            with self.conn_or_acquire(retry=False) as client:
                if client:
                    client.lpush(self.keys[0], dumps(value))
                    return
        except ConnectionError:
            pass
        raise Queue.Full
