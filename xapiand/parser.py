from __future__ import unicode_literals, absolute_import

import re

from . import json


SPLIT_RE = re.compile(r'\s*[,;]\s*|\s+')

OFFSET_RE = re.compile(r'\bOFFSET\s+(\d+)\b', re.IGNORECASE)
LIMIT_RE = re.compile(r'\bLIMIT\s+(\d+)\b', re.IGNORECASE)
ORDER_BY_RE = re.compile(r'\bORDER\s+BY\s+([-+_a-zA-Z0-9, ]+?)(\s+ASC\b|\s+DESC\b|$)', re.IGNORECASE)
FACETS_RE = re.compile(r'\bFACETS\s+(\d+)?([_a-zA-Z0-9, ]+)\b', re.IGNORECASE)
PARTIAL_RE = re.compile(r'\bPARTIAL\s+(.*)', re.IGNORECASE)
SEARCH_RE = re.compile(r'\bSEARCH\s+(.*)', re.IGNORECASE)
TERMS_RE = re.compile(r'\bTERMS\s+(.*)', re.IGNORECASE)
DISTINCT_RE = re.compile(r'\bDISTINCT\s+(.*)', re.IGNORECASE)

CMDS_RE = re.compile(r'\b(OFFSET|LIMIT|ORDER\s+BY|FACETS|PARTIAL|SEARCH|TERMS|DISTINCT)\s', re.IGNORECASE)


def index_parser(document):
    """
    Format of the JSON object to index (only "id" and "data" are required):
        {
            "id": "<document_id_string>",
            "data": <data_object>,

            "values": {
                "<field_name_N>": <value_N>,
                ...
            },
            "terms": [
                {
                    "term": "<term_N_string>",
                    "weight": [term_N_weight],
                    "prefix": "[term_N_prefix]",
                    "position": [term_N_position]
                },
                ...
            ],
            "texts": [
                {
                    "text": "<text_N_string>",
                    "weight": [text_N_weight],
                    "prefix": "[text_N_prefix]",
                    "language": [text_N_language]
                },
                ...
            ],
            "endpoints": [
                "<endpoint_N>",
                ...
            ],
            "language": "<language>",
            "spelling": False,
            "positions": False
        }

    """
    try:
        if not isinstance(document, dict):
            document = json.loads(document)
            if not isinstance(document, dict):
                raise ValueError("Document must be a dictionary")
    except Exception as e:
        return ">> ERR: [400] %s" % e

    document = document.copy()
    document_id = document.pop('id', None)
    if not document_id:
        return ">> ERR: [400] Document must have an 'id'"

    document_data = document.pop('data', None)
    if not document_data:
        return ">> ERR: [400] You must provide 'data' to index"
    try:
        document_data = json.dumps(document_data, ensure_ascii=False).encode('safe-utf-8')
    except Exception:
        return ">> ERR: [400] 'data' must be a valid json serializable object"

    default_language = document.pop('language', None)
    default_spelling = bool(document.pop('spelling', False))
    default_positions = bool(document.pop('positions', False))

    try:
        endpoints = document.pop('endpoints')
        if not isinstance(endpoints, list):
            return ">> ERR: [400] 'endpoints' must be a list"
        endpoints = tuple(endpoints)
    except KeyError:
        endpoints = None

    document_values = document.pop('values', {})
    if not isinstance(document_values, dict):
        return ">> ERR: [400] 'values' must be an object"

    document_terms = []
    _document_terms = document.pop('terms', [])
    if not isinstance(_document_terms, list):
        return ">> ERR: [400] 'terms' must be a list of objects"
    for texts in _document_terms:
        try:
            get = texts.get
        except AttributeError:
            return ">> ERR: [400] 'terms' must be a list of objects"
        term = get('term')
        if not term:
            return ">> ERR: [400] 'term' is required"
        weight = get('weight')
        prefix = get('prefix')
        position = get('position')
        document_terms.append((term, weight, prefix, position))

    document_texts = []
    _document_texts = document.pop('texts', [])
    if not isinstance(_document_texts, list):
        return ">> ERR: [400] 'texts' must be a list of objects"
    for texts in _document_texts:
        try:
            get = texts.get
        except AttributeError:
            return ">> ERR: [400] 'texts' must be a list of objects"
        text = get('text')
        if not text:
            return ">> ERR: [400] 'text' is required"
        weight = get('weight')
        prefix = get('prefix')
        language = get('language')
        spelling = get('spelling')
        positions = get('positions')
        if language == default_language:
            language = None
        if spelling == default_spelling:
            spelling = None
        if positions == default_positions:
            positions = None
        document_texts.append((text, weight, prefix, language, spelling, positions))

    if document:
        return ">> ERR: [400] Unknown document fields: %s" % ', '.join(document)

    document = (
        document_id,
        document_values,
        document_terms,
        document_texts,
        document_data,
        default_language,
        default_spelling,
        default_positions,
    )

    return endpoints, document


def search_parser(query_string):
    """
    The query can have any or a mix of:
        SEARCH query_string
        PARTIAL <partial ...> [PARTIAL <partial ...>]...
        TERMS <term ...>
        FACETS <min> <field_name ...>
        OFFSET <offset>
        LIMIT <limit>
        ORDER BY <field_name ...> [ASC|DESC]
    """
    try:
        query_string = json.loads(query_string)
    except (ValueError, TypeError):
        pass
    if isinstance(query_string, dict):
        return query_string

    first = 0
    partials = []
    maxitems = 10000
    check_at_least = 0
    sort_by = None
    sort_by_reversed = None
    search = []
    facets = None
    terms = []
    distinct = False

    query_string = " SEARCH %s " % (query_string or '')
    query_re = r''.join('(%s.*)' % s for s in CMDS_RE.findall(query_string))

    for string in re.search(query_re, query_string).groups():

        # Get first item (OFFSET):
        match = OFFSET_RE.search(string)
        if match:
            string = OFFSET_RE.sub('', string)
            first = int(match.group(1))
            # print "offset:", first
            continue

        # Get maximum number of items (LIMIT):
        match = LIMIT_RE.search(string)
        if match:
            string = LIMIT_RE.sub('', string)
            maxitems = int(match.group(1))
            # print "limit:", maxitems
            continue

        # Get wanted order by:
        match = ORDER_BY_RE.search(string)
        if match:
            string = ORDER_BY_RE.sub('', string)
            sort_by = SPLIT_RE.split(match.group(1).strip())
            sort_by_reversed = match.group(2) == 'DESC'
            # print "order:", sort_by
            continue

        # Get wanted facets:
        match = FACETS_RE.search(string)
        if match:
            string = FACETS_RE.sub('', string)
            facets = SPLIT_RE.split(match.group(2).strip())
            if facets:
                if match.group(1):
                    check_at_least = max(min(int(match.group(1)), 10000), 0)
            else:
                check_at_least = 0
            # print "facets:", facets
            continue

        # Get partials (for autocomplete):
        match = PARTIAL_RE.search(string)
        if match:
            string = PARTIAL_RE.sub('', string)
            v = match.group(1).strip()
            if v:
                partials.append(v)
            # print "partials:", partials
            continue

        # Get terms filtering:
        match = TERMS_RE.search(string)
        if match:
            string = TERMS_RE.sub('', string)
            v = match.group(1).strip()
            if v:
                terms.append(v)
            # print "terms:", terms
            continue

        match = DISTINCT_RE.search(string)
        if match:
            string = DISTINCT_RE.sub('', string)
            v = match.group(1).strip()
            if v:
                distinct = v
            else:
                distinct = True
            # print "distinct:", distinct
            continue

        # Get searchs:
        match = SEARCH_RE.search(string)
        if match:
            string = SEARCH_RE.sub('', string)
            v = match.group(1).strip()
            if v:
                search.append(v)
            # print "search:", search
            continue

    return {
        'search': search,
        'facets': facets,
        'terms': terms,
        'partials': partials,
        'first': first,
        'maxitems': maxitems,
        'sort_by': sort_by,
        'distinct': distinct,

        'sort_by_reversed': sort_by_reversed,
        'check_at_least': check_at_least,
    }
