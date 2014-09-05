from __future__ import unicode_literals, absolute_import

import re
import datetime

try:
    from dateutil.tz import tzoffset
except ImportError:
    tzoffset = None

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
            parts.query,
            dict(parse_qsl(parts.query)))


def build_url(scheme, hostname, port, username, password, path, query, query_dict):
    return ''.join((
        "%s://" % scheme if scheme else '',
        username or '',
        ":%s" % password if hostname and username and password else '',
        "@%s" % hostname if hostname and (username or password) else hostname or '',
        ":%s" % port if port else '',
        "/%s" % path if path and hostname else path or '',
        "?%s" % query if query and hostname else '',
    ))


if not tzoffset:
    ZERO = datetime.timedelta(0)

    class tzoffset(datetime.tzinfo):
        def __init__(self, name, offset):
            self._name = name
            self._offset = datetime.timedelta(seconds=offset)

        def utcoffset(self, dt):
            return self._offset

        def dst(self, dt):
            return ZERO

        def tzname(self, dt):
            return self._name

        def __eq__(self, other):
            return (isinstance(other, tzoffset) and
                    self._offset == other._offset)

        def __ne__(self, other):
            return not self.__eq__(other)

        def __repr__(self):
            return "%s(%s, %s)" % (self.__class__.__name__,
                                   repr(self._name),
                                   self._offset.days * 86400 + self._offset.seconds)


def isoparse(ds):
    try:
        dt = datetime.datetime.strptime(ds[:26].replace(' ', 'T'), '%Y-%m-%dT%H:%M:%S.%f')
        try:
            offset = ds[26:].replace(':', '')
            delta = datetime.timedelta(hours=int(offset[:-2]), minutes=int(offset[-2:]))
            dt = dt.replace(tzinfo=tzoffset(None, int(delta.total_seconds())))
        except:
            pass
    except:
        try:
            dt = datetime.datetime.strptime(ds[:19].replace(' ', 'T'), '%Y-%m-%dT%H:%M:%S')
            try:
                offset = ds[19:].replace(':', '')
                delta = datetime.timedelta(hours=int(offset[:-2]), minutes=int(offset[-2:]))
                dt = dt.replace(tzinfo=tzoffset(None, int(delta.total_seconds())))
            except:
                pass
        except:
            try:
                dt = datetime.datetime.strptime(ds[:15], '%H:%M:%S.%f')
            except:
                try:
                    dt = datetime.datetime.strptime(ds[:8], '%H:%M:%S')
                except:
                    try:
                        dt = datetime.datetime.strptime(ds[:8], '%H:%M')
                    except:
                        dt = datetime.datetime.strptime(ds[:10], '%Y-%m-%d')
    return dt


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
