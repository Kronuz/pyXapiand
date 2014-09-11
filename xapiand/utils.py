from __future__ import unicode_literals, absolute_import, division

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
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            pass
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        try:
            dt = datetime.datetime.strptime(ds[:19].replace(' ', 'T'), '%Y-%m-%dT%H:%M:%S')
            try:
                offset = ds[19:].replace(':', '')
                delta = datetime.timedelta(hours=int(offset[:-2]), minutes=int(offset[-2:]))
                dt = dt.replace(tzinfo=tzoffset(None, int(delta.total_seconds())))
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                pass
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            try:
                dt = datetime.datetime.strptime(ds[:15], '%H:%M:%S.%f')
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                try:
                    dt = datetime.datetime.strptime(ds[:8], '%H:%M:%S')
                except (KeyboardInterrupt, SystemExit):
                    raise
                except:
                    try:
                        dt = datetime.datetime.strptime(ds[:8], '%H:%M')
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except:
                        dt = datetime.datetime.strptime(ds[:10], '%Y-%m-%d')
    return dt


def format_time(timespan, precision=3):
    """Formats the timespan in a human readable form"""
    import math

    if timespan >= 60.0:
        # we have more than a minute, format that in a human readable form
        # Idea from http://snipplr.com/view/5713/
        parts = [("d", 60 * 60 * 24), ("h", 60 * 60), ("min", 60), ("s", 1)]
        time = []
        leftover = timespan
        for suffix, length in parts:
            value = int(leftover / length)
            if value > 0:
                leftover = leftover % length
                time.append(u'%s%s' % (str(value), suffix))
            if leftover < 1:
                break
        return " ".join(time)

    units = ["s", "ms", '\xb5s', "ns"]  # the save value
    scaling = [1, 1e3, 1e6, 1e9]

    if timespan > 0.0:
        order = min(-int(math.floor(math.log10(timespan)) // 3), 3)
    else:
        order = 3
    ret = "%.*g %s" % (precision, timespan * scaling[order], units[order])
    return ret
