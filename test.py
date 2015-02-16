#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function, division

import sys
import time
import Queue
import random
import threading
import lorem_ipsum

from xapiand import Xapian

DATABASE = 'example'
POOL_SIZE = 35
THREADS = 10

TOTAL_DOCS = 5000
LIMIT = 50

xapian = Xapian('localhost:8890', max_pool_size=POOL_SIZE)

queue = Queue.Queue()


class Search(threading.Thread):
    running = True

    def run(self):
        queue.put(self)
        try:
            while Search.running:
                self._run()
                time.sleep(random.random() * 0.2)
        except Exception:
            Search.running = False
            raise
        finally:
            queue.get()
            queue.task_done()

    def _run(self):
        i = 0
        done = False
        try:
            conn = xapian.checkout()
            conn.using([DATABASE])
            offset = random.randint(0, TOTAL_DOCS // 2)
            results = conn.search('all', offset=offset, limit=LIMIT)
            max_results = random.randint(LIMIT // 3, LIMIT * 2)
            expected = min(LIMIT, max_results)
            for result in results:
                if i >= max_results or not Search.running:
                    break
                try:
                    data = result['data']
                except KeyError:
                    print("%s - UNEXPECTED RESULT: %r" % (self.name, result), file=sys.stderr)
                    # raise
                print("%s - %d. %s" % (self.name, i, data))
                i += 1
            if i != expected:
                print("%s - MISSING RESULTS! (received %d out of expected %d)" % (self.name, i, expected), file=sys.stderr)
            else:
                done = True
        except:
            raise
        else:
            print("%s - Print %d/%d (%d-%d) results received %s" % (self.name, i, expected, offset, LIMIT, "(all)" if done else "(truncated)"), file=sys.stderr)


def usage():
    """
    Stress test Xapiand.

    usage: {cmd} <build|search>

        build: {build}
        search: {search}
    Note: Xapiand must already be running. (To run Xapiand, use ``python -m xapiand -vv``)

    """
    doc = usage.__doc__
    doc = '\n'.join(l[4:] for l in doc.split('\n')).lstrip()
    cmds = dict((name, obj.__doc__) for name, obj in globals().items() if hasattr(obj, '__doc__'))
    doc = doc.format(cmd=sys.argv[0], **cmds)
    print(doc, file=sys.stderr)


def build():
    """
        Builds a stress test database.
        run as ``test.py build``

    """
    words = lorem_ipsum.words(10).split()
    with xapian.connection() as conn:
        conn.using([DATABASE])
        for i in range(TOTAL_DOCS):
            doc = {
                "id": "doc%d" % i,
                "data": lorem_ipsum.sentence(),
                "terms": [{"term": "all"}] + [{"term": random.choice(words)} for i in range(random.randint(1, 4))],
            }
            conn.index(doc)


def search():
    """
        This command creates THREADS threads and makes a stress search on the database.
        The database should already exist (can be created with ``test.py build``).
        run as ``test.py search``

    """
    for i in range(THREADS):
        Search(name="Thread%d" % i).start()

    try:
        while not queue.empty():
            time.sleep(1)
    except KeyboardInterrupt:
        Search.running = False

    queue.join()


if __name__ == '__main__':
    try:
        globals()[sys.argv[1]]()
    except (IndexError, KeyError):
        usage()
