from __future__ import unicode_literals, absolute_import

import re
import json

SPLIT_RE = re.compile(r'\s*,\s*|\s+')

OFFSET_RE = re.compile(r'\bOFFSET\s+(\d+)\b', re.IGNORECASE)
LIMIT_RE = re.compile(r'\bLIMIT\s+(\d+)\b', re.IGNORECASE)
ORDER_BY_RE = re.compile(r'\bORDER\s+BY\s+([-+_a-zA-Z0-9, ]+)(\s+ASC|\s+DESC)\b', re.IGNORECASE)
FACETS_RE = re.compile(r'\bFACETS\s+(\d+)?([_a-zA-Z0-9, ]+)\b', re.IGNORECASE)
PARTIAL_RE = re.compile(r'\bPARTIAL\s+([_a-zA-Z0-9, *]+)\b', re.IGNORECASE)
SEARCH_RE = re.compile(r'\bSEARCH\s+(.+)\b', re.IGNORECASE)
TERMS_RE = re.compile(r'\bTERMS\s+(.+)\b', re.IGNORECASE)

CMDS_RE = re.compile(r'\b(OFFSET|LIMIT|ORDER\s+BY|FACETS|PARTIAL|SEARCH|TERMS)\s', re.IGNORECASE)


def index_parser(document):
    """
    Receives a json dictionary to index.
        "id" is the only required field

    {
        "id": <DOCUMENT_ID_STRING>,
        "data": <DATA_OBJECT>,

        "endpoints": [
            <ENDPOINT_n>,
            ...
        ],
        "values": {
            <KEY_n>: <VALUE_n>,
            ...
        },
        "terms": [
            {
                "term": <TERM_n_STRING>,
                "weight": <TERM_n_WEIGHT>,
                "prefix": <TERM_n_PREFIX>
            },
            ...
        ],
        "texts": [
            {
                "text": <TEXT_n_STRING>,
                "weight": <TEXT_n_WEIGHT>,
                "prefix": <TEXT_n_PREFIX>
                "language": <TEXT_n_LANGUAGE>
            },
            ...
        ]
        "language": <LANGUAGE>,
        "spelling: False,
        "positions: False,
    }

    """
    try:
        if not isinstance(document, dict):
            document = json.loads(document)
            if not isinstance(document, dict):
                raise ValueError("Document must be a dictionary")
    except Exception as e:
        return ">> ERR: [400] %s" % e

    document_id = document.get('id')
    if not document_id:
        return ">> ERR: [400] Document must have an 'id'"

    document_data = document.get('data')
    if not document_data:
        return ">> ERR: [400] You must provide 'data' to index"
    try:
        document_data = json.dumps(document_data)
    except:
        return ">> ERR: [400] 'data' must be a valid json serializable object"

    default_language = document.get('language')
    default_spelling = bool(document.get('spelling', False))
    default_positions = bool(document.get('positions', False))

    try:
        endpoints = document['endpoints']
        if not isinstance(endpoints, list):
            return ">> ERR: [400] 'endpoints' must be a list"
    except KeyError:
        endpoints = None

    document_values = []
    _document_values = document.get('values', {})
    if not isinstance(_document_values, dict):
        return ">> ERR: [400] 'values' must be an object"

    document_terms = []
    _document_terms = document.get('terms', [])
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
        document_terms.append((term, weight, prefix))

    document_texts = []
    _document_texts = document.get('texts', [])
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
    first = 0
    partials = []
    maxitems = 10000
    check_at_least = 10000
    sort_by = None
    sort_by_reversed = None
    search = None
    spies = None
    terms = None

    query_string = 'SEARCH %s' % query_string
    query_re = r''.join('(%s.*)' % s for s in CMDS_RE.findall(query_string))

    for string in re.search(query_re, query_string).groups():

        # Get first item (OFFSET):
        match = OFFSET_RE.search(string)
        if match:
            string = OFFSET_RE.sub('', string)
            first = int(match.group(1))
            # log.debug("offset: %s", first)
            continue

        # Get maximum number of items (LIMIT):
        match = LIMIT_RE.search(string)
        if match:
            string = LIMIT_RE.sub('', string)
            maxitems = int(match.group(1))
            # log.debug("limit: %s", maxitems)
            continue

        # Get wanted order by:
        match = ORDER_BY_RE.search(string)
        if match:
            string = ORDER_BY_RE.sub('', string)
            sort_by = SPLIT_RE.split(match.group(1).strip())
            sort_by_reversed = match.group(2) == 'DESC'
            # log.debug("order: %s", sort_by)
            continue

        # Get wanted facets:
        match = FACETS_RE.search(string)
        if match:
            string = FACETS_RE.sub('', string)
            spies = SPLIT_RE.split(match.group(2).strip())
            if spies:
                check_at_least = int(match.group(1))
            else:
                check_at_least = 0
            # log.debug("facets: %s", spies)
            continue

        # Get partials (for autocomplete):
        match = PARTIAL_RE.search(string)
        if match:
            string = PARTIAL_RE.sub('', string)
            partials.append(match.group(1).strip())
            # log.debug("partials: %s", partials)
            continue

        # Get terms filtering:
        match = TERMS_RE.search(string)
        if match:
            string = TERMS_RE.sub('', string)
            terms = match.group(1).strip()
            # log.debug("terms: %s", terms)
            continue

        # Get searchs:
        match = SEARCH_RE.search(string)
        if match:
            string = SEARCH_RE.sub('', string)
            search = match.group(1).strip()
            # log.debug("search: %s", search)
            continue

    parsed = (
        first,
        maxitems,
        sort_by,
        sort_by_reversed,
        spies,
        check_at_least,
        partials,
        terms,
        search,
    )

    return parsed
