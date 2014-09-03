from __future__ import unicode_literals, absolute_import

import unicodedata


def colored_logging(logging):
    """
    Colored logging.

    http://stackoverflow.com/questions/384076/how-can-i-color-python-logging-output

    """
    def add_coloring_to_emit_ansi(fn):
        # add methods we need to the class
        def new(*args):
            levelno = args[1].levelno
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
            args[1].msg = u"%s%s\x1b[0m" % (color, args[1].msg)  # normal
            return fn(*args)
        return new
    logging.StreamHandler.emit = add_coloring_to_emit_ansi(logging.StreamHandler.emit)


def normalize(text):
    """
    Utility method that converts strings to strings without accents and stuff.

    """
    return ''.join(c for c in unicodedata.normalize('NFKD', unicode(text)) if not unicodedata.combining(c))
