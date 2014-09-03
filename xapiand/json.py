# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals
"""
Xapiand
~~~~~~~

:author: German M. Bravo (Kronuz)
:copyright: Copyright (c) 2014, German M. Bravo. All Rights Reserved.
:license: See LICENSE for license details.

"""
import re
import codecs
import datetime
import decimal
import uuid
import dateutil.parser

try:
    import simplejson as json
except ImportError:
    import json

from .serialise import LatLongCoord

try:
    JSONDecodeError = json.JSONDecodeError
except AttributeError:
    JSONDecodeError = ValueError


class XapianJSONEncoder(json.JSONEncoder):
    """
    JSONEncoder subclass that knows how to encode date/time, decimal and (latitude, longitude) tuple types.

    """

    def time_repr(o):
            if o.tzinfo is not None and o.tzinfo.utcoffset(o) is not None:
                raise ValueError("JSON can't represent timezone-aware times.")
            r = o.isoformat()
            if o.microsecond:
                r = r[:12]
            return r

    ENCODER_BY_TYPE = {
        uuid.UUID: lambda o: "%s" % o,
        datetime.datetime: lambda o: o.isoformat(),
        datetime.date: lambda o: o.isoformat(),
        datetime.time: time_repr,
        decimal.Decimal: lambda o: "%s" % o,
        LatLongCoord: lambda o: "(%s, %s)" % (o.latitude, o.longitude),
        set: list,
        frozenset: list,
        bytes: lambda o: o.decode('utf-8', errors='replace'),
    }

    def default(self, obj):
        try:
            return self.ENCODER_BY_TYPE[type(obj)](obj)
        except:
            return super(XapianJSONEncoder, self).default(obj)


DECODER_BY_RE = (
    (re.compile(r'[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$'), uuid.UUID),
    (re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'), dateutil.parser.parse),
    (re.compile(r'\d{4}-\d{2}-\d{2}$'), lambda o: dateutil.parser.parse(o).date()),
    (re.compile(r'\d{2}:\d{2}:\d{2}$'), lambda o: dateutil.parser.parse(o).time()),
    (re.compile(r'\([-+]?(\d+|\d*.\d+),\s*[-+]?(\d+|\d*.\d+)\)$'), lambda o: LatLongCoord(*eval(o))),
)


def parse_string(s):
    for r, d in DECODER_BY_RE:
        if r.match(s):
            try:
                return d(s)
            except:
                pass
    raise ValueError


def xapian_decoder(data):
    for k, v in data.items():
        if isinstance(v, basestring):
            try:
                data[k] = parse_string(v)
            except ValueError:
                pass
    return data


def dump(obj, fp, **kwargs):
    if 'ensure_ascii' not in kwargs:
        kwargs['ensure_ascii'] = False
    if 'encoding' not in kwargs:
        kwargs['encoding'] = 'safe-utf-8'
    return json.dump(obj, fp, cls=XapianJSONEncoder, **kwargs)


def dumps(value, **kwargs):
    ensure_ascii = kwargs.pop('ensure_ascii', True)
    kwargs['ensure_ascii'] = False
    if 'encoding' not in kwargs:
        kwargs['encoding'] = 'safe-utf-8'
    dump = json.dumps(value, cls=XapianJSONEncoder, **kwargs)
    if ensure_ascii:
        dump = dump.encode('safe-utf-8')
    return dump


def load(fp, **kwargs):
    return json.load(fp, object_hook=xapian_decoder, **kwargs)


def loads(value, **kwargs):
    return json.loads(value, object_hook=xapian_decoder, **kwargs)


_utf8_encoder = codecs.getencoder('utf-8')


def safe_encode(input, errors='backslashreplace'):
    return _utf8_encoder(input, errors)


_utf8_decoder = codecs.getdecoder('utf-8')


def safe_decode(input, errors='replace'):
    return _utf8_decoder(input, errors)


class Codec(codecs.Codec):

    def encode(self, input, errors='backslashreplace'):
        return safe_encode(input, errors)

    def decode(self, input, errors='replace'):
        return safe_decode(input, errors)


class IncrementalEncoder(codecs.IncrementalEncoder):
    def encode(self, input, final=False):
        return safe_encode(input, self.errors)[0]


class IncrementalDecoder(codecs.IncrementalDecoder):
    def decode(self, input, final=False):
        return safe_decode(input, self.errors)[0]


class StreamWriter(Codec, codecs.StreamWriter):
    pass


class StreamReader(Codec, codecs.StreamReader):
    pass


def getregentry(name):
    if name != 'safe-utf-8':
        return None
    return codecs.CodecInfo(
        name='safe-utf-8',
        encode=safe_encode,
        decode=safe_decode,
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )


codecs.register(getregentry)
