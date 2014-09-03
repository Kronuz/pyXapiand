from __future__ import absolute_import, unicode_literals

from multiprocessing.queues import Queue

import logging
logger = logging.getLogger(__name__)


class MemoryQueue(Queue):
    def __init__(self, name=None, log=None):
        self.name = name
        self.log = log or logger
        super(MemoryQueue, self).__init__()
