from __future__ import unicode_literals, absolute_import

import re

try:
    from urllib.parse import unquote, urlparse, parse_qsl
except ImportError:
    from urllib import unquote                  # NOQA
    from urlparse import urlparse, parse_qsl    # NOQA


_MULTIPLE_PATHS = re.compile(r'/{2,}')


def normalize_path(path):
    if path:
        path = path.replace('\\', '/')
        path = _MULTIPLE_PATHS.sub('/', path)
        if path[-1] == '/':
            path = path[:-1]
        if path.startswith('./'):
            path = path[2:]
    return path


def parse_url(url):
    parts = urlparse(url)
    hostname = unquote(parts.hostname or '')
    path = unquote(parts.path or '')
    if parts.scheme:
        path = path.lstrip('/')
        scheme = parts.scheme
        if scheme == 'file':
            path, hostname = hostname + '/' + path, ''
    else:
        scheme = 'file'
    return (scheme,
            hostname or None,
            parts.port or None,
            unquote(parts.username or '') or None,
            unquote(parts.password or '') or None,
            normalize_path(path) or None,
            dict(parse_qsl(parts.query)))


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
