import datetime
import unicodedata

try:
    from xapian import LatLongCoord
except ImportError:
    class LatLongCoord(object):
        def __init__(self, latitude, longitude=None):
            if longitude is None:
                latitude, longitude = latitude
            self.latitude = float(latitude)
            self.longitude = float(longitude)

        def serialise(self):
            return "(%s,%s)" % (self.latitude, self.longitude)

        def __repr__(self):
            return b"%s(%s, %s)" % (self.__class__.__name__, self.latitude, self.longitude)

try:
    from xapian import sortable_serialise
except ImportError:
    def sortable_serialise(value):
        return "%s" % value


def normalize(text):
    """
    Utility method that converts strings to strings without accents and stuff.

    """
    return ''.join(c for c in unicodedata.normalize('NFKD', unicode(text)) if not unicodedata.combining(c))


def serialise_value(value):
    """
    Utility method that converts Python values to a string for Xapian values.

    """
    values = []
    if isinstance(value, datetime.datetime):
        if value.microsecond:
            value = '%04d%02d%02d%02d%02d%02d%06d' % (
                value.year, value.month, value.day, value.hour,
                value.minute, value.second, value.microsecond
            )
        else:
            value = '%04d%02d%02d%02d%02d%02d' % (
                value.year, value.month, value.day, value.hour,
                value.minute, value.second
            )
        values.append(value)
    elif isinstance(value, datetime.date):
        value = '%04d%02d%02d000000' % (value.year, value.month, value.day)
        values.append(value)
    elif isinstance(value, datetime.time):
        if value.microsecond:
            value = '%02d%02d%02d%06d' % (
                value.hour, value.minute, value.second, value.microsecond
            )
        else:
            value = '%02d%02d%02d' % (
                value.hour, value.minute, value.second
            )
        values.append(value)
    elif isinstance(value, bool):
        values.append('t' if value else 'f')
    elif isinstance(value, float):
        values.append(sortable_serialise(value))
    elif isinstance(value, (int, long)):
        values.append('%012d' % value)
    elif isinstance(value, LatLongCoord):
        value = value.serialise()
        values.append(value)
        for term, value in [(value[:-i], 5 - i) if i else (value, 5) for i in range(5)]:
            values.append(value)
    elif hasattr(value, 'serialise'):
        values.append(value.serialise())
    elif value:
        values.append(normalize("%s" % value))
    else:
        values.append('')
    return values
