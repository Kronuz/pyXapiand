from __future__ import absolute_import, unicode_literals

import logging


class QueueHandler(logging.Handler):
    """
    This is a logging handler which sends events to a multiprocessing queue.

    """
    def __init__(self, queue):
        """
        Initialise an instance, using the passed queue.
        """
        logging.Handler.__init__(self)
        self.queue = queue

    def emit(self, record):
        """
        Emit a record.

        Writes the LogRecord to the queue.
        """
        try:
            ei = record.exc_info
            if ei:
                self.format(record)  # just to get traceback text into record.exc_text
                record.exc_info = None  # not needed any more
            self.queue.put_nowait(record)
        except Exception:
            self.handleError(record)


class ColoredStreamHandler(logging.StreamHandler):
    """
    Colored logging.

    http://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output

    """
    def emit(self, record):
        levelno = record.levelno
        if(levelno >= 50):
            color = "\x1b[1;37;41m"  # red
        elif(levelno >= 40):
            color = "\x1b[1;31m"  # red
        elif(levelno >= 30):
            color = "\x1b[0;33m"  # yellow
        elif(levelno >= 20):
            color = "\x1b[0;36m"  # cyan
        elif(levelno >= 10):
            color = "\x1b[1;30m"  # darkgrey
        else:
            color = "\x1b[0m"  # normal
        record.msg = u"%s%s\x1b[0m" % (color, record.msg)  # normal
        super(ColoredStreamHandler, self).emit(record)
