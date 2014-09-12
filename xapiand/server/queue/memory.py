from __future__ import absolute_import, unicode_literals

from gevent.queue import Queue

import logging


class MemoryQueue(Queue):
    persistent = False

    def __init__(self, name=None, log=logging):
        self.name = name
        self.log = log
        super(MemoryQueue, self).__init__()
