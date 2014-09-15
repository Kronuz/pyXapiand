from __future__ import unicode_literals, absolute_import

import base64
import logging

import xapian

from . import json
from .core import xapian_reopen, get_slot, expand_terms, find_terms, get_document, get_data, get_value, get_termlist, DOCUMENT_CUSTOM_TERM_PREFIX
from .serialise import normalize, serialise_value
from .exceptions import XapianError

MAX_DOCS = 10000


class Search(object):
    def __init__(self, database, search,
                 get_matches=True, get_data=True, get_terms=False, get_size=False,
                 data='.', log=logging, dead=False):
        self.database = database
        self.search = search

        self.get_matches = get_matches
        self.get_terms = get_terms
        self.get_data = get_data
        self.get_size = get_size

        self.data = data
        self.log = log
        self.dead = dead

        self.spies = {}
        self.warnings = []
        self.produced = 0

        self.size = None
        self.facets = self.search.get('facets')
        self.check_at_least = self.search.get('check_at_least', MAX_DOCS if self.facets else 0)
        self.maxitems = self.search.get('maxitems', MAX_DOCS)
        self.first = self.search.get('first', 0)

        self.setup()

    def setup(self):
        queryparser = xapian.QueryParser()
        queryparser.set_database(self.database)

        prefixes = set()

        def add_prefixes(string):
            for term, term_field, terms in find_terms(string):
                if term_field and term_field not in prefixes:
                    prefix = '%s%s' % (DOCUMENT_CUSTOM_TERM_PREFIX, get_slot(term_field))
                    if term_field.lower() == term_field:
                        queryparser.add_prefix(term_field, prefix)
                    else:
                        queryparser.add_boolean_prefix(term_field, prefix)
                    prefixes.add(term_field)

        # Build final query:
        search = self.search.get('search')
        if search:
            if not isinstance(search, (tuple, list)):
                search = [search]
            search = " AND ".join("(%s)" % s for s in search if s)
        if search and search != '(*)':
            search = normalize(search).encode('utf-8')

            ranges = self.search.get('ranges')
            if ranges:
                _ranges = set()
                for field, begin, end in ranges:
                    field = field.encode('utf-8')
                    if field not in _ranges:
                        slot = get_slot(field)
                        vrp = xapian.StringValueRangeProcessor(slot, field)
                        queryparser.add_valuerangeprocessor(vrp)
                        _ranges.add(field)
                    if begin is None:
                        begin = b''
                    if end is None:
                        end = b''
                    rng1 = b'(%s:%s..%s)' % (field, begin, end)
                    rng2 = b'(%s:%s..%s)' % (field, serialise_value(begin)[0], serialise_value(end)[0])
                    if rng1 == rng2:
                        _search = search
                        if rng1 in search:
                            search = None
                    else:
                        _search = search.replace(rng1, rng2)
                    if search != _search:
                        search = _search
                    else:
                        search += b' AND %s' % rng2

            search = expand_terms(search)
            add_prefixes(search)
            flags = xapian.QueryParser.FLAG_DEFAULT | xapian.QueryParser.FLAG_WILDCARD | xapian.QueryParser.FLAG_PURE_NOT
            try:
                query = queryparser.parse_query(search, flags)
            except (xapian.NetworkError, xapian.DatabaseModifiedError):
                self.database = xapian_reopen(self.database, data=self.data, log=self.log)
                queryparser.set_database(self.database)
                query = queryparser.parse_query(search, flags)

        partials = self.search.get('partials')
        if partials:
            if not isinstance(partials, (tuple, list)):
                partials = [partials]
            # Partials (for autocomplete) using FLAG_PARTIAL and OP_AND_MAYBE
            partials_query = None
            for partial in partials:
                self.dead or 'alive'  # Raises DeadException when needed
                partial = normalize(partial)
                partial = expand_terms(partial)
                add_prefixes(partial)
                flags = xapian.QueryParser.FLAG_PARTIAL
                try:
                    _partials_query = queryparser.parse_query(partial, flags)
                except (xapian.NetworkError, xapian.DatabaseModifiedError):
                    self.database = xapian_reopen(self.database, data=self.data, log=self.log)
                    queryparser.set_database(self.database)
                    _partials_query = queryparser.parse_query(partial, flags)
                if partials_query:
                    partials_query = xapian.Query(
                        xapian.Query.OP_AND_MAYBE,
                        partials_query,
                        _partials_query,
                    )
                else:
                    partials_query = _partials_query
            if query:
                query = xapian.Query(
                    xapian.Query.OP_AND,
                    query,
                    partials_query,
                )
            else:
                query = partials_query

        terms = self.search.get('terms')
        if terms:
            if not isinstance(terms, (tuple, list)):
                terms = [terms]
            for term in terms:
                term = normalize(term)
                term = expand_terms(term)
                add_prefixes(term)
                flags = xapian.QueryParser.FLAG_BOOLEAN | xapian.QueryParser.FLAG_PURE_NOT
                try:
                    terms_query = queryparser.parse_query(term, flags)
                except (xapian.NetworkError, xapian.DatabaseModifiedError):
                    self.database = xapian_reopen(self.database, data=self.data, log=self.log)
                    queryparser.set_database(self.database)
                    terms_query = queryparser.parse_query(term, flags)
                if query:
                    query = xapian.Query(
                        xapian.Query.OP_AND,
                        query,
                        terms_query,
                    )
                else:
                    query = terms_query

        if not query:
            if search == '(*)':
                query = xapian.Query('')
            else:
                query = xapian.Query()

        self.query = query
        self.sort_by = self.search.get('sort_by')
        self.distinct = self.search.get('distinct')
        self.sort_by_reversed = self.search.get('sort_by_reversed')

    def get_enquire(self):
        enquire = xapian.Enquire(self.database)
        # enquire.set_weighting_scheme(xapian.BoolWeight())
        # enquire.set_docid_order(xapian.Enquire.DONT_CARE)
        # if weighting_scheme:
        #     enquire.set_weighting_scheme(xapian.BM25Weight(*self.weighting_scheme))
        enquire.set_query(self.query)

        spies = {}
        sort_by = []
        warnings = []

        if self.facets:
            for name in self.facets:
                self.dead or 'alive'  # Raises DeadException when needed
                name = name.strip().lower()
                slot = get_slot(name)
                if slot:
                    spy = xapian.ValueCountMatchSpy(slot)
                    enquire.add_matchspy(spy)
                    spies[name] = spy
                else:
                    warnings.append("Ignored document value name (%r)" % name)

        if self.sort_by:
            for sort_field in self.sort_by:
                self.dead or 'alive'  # Raises DeadException when needed
                if sort_field.startswith('-'):
                    reverse = True
                    sort_field = sort_field[1:]  # Strip the '-'
                else:
                    reverse = False
                sort_by.append((sort_field, reverse))

            sorter = xapian.MultiValueKeyMaker()
            for name, reverse in sort_by:
                self.dead or 'alive'  # Raises DeadException when needed
                name = name.strip().lower()
                slot = get_slot(name)
                if slot:
                    sorter.add_value(slot, reverse)
                else:
                    warnings.append("Ignored document value name (%r)" % name)
            enquire.set_sort_by_key_then_relevance(sorter, self.sort_by_reversed)

        if self.distinct:
            if self.distinct is True:
                field = 'id'
            else:
                field = self.distinct
            enquire.set_collapse_key(get_slot(field))
        self.spies = spies
        self.warnings = warnings

        return enquire

    def get_results(self):
        try:
            doccount = self.database.get_doccount()
        except (xapian.NetworkError, xapian.DatabaseModifiedError):
            database = xapian_reopen(self.database, data=self.data, log=self.log)
            if self.database != database:
                self.database = database
            doccount = self.database.get_doccount()

        maxitems = max(min(self.maxitems, doccount - self.first, MAX_DOCS), 0)
        check_at_least = max(min(self.check_at_least, doccount, MAX_DOCS), 0)

        if not self.get_matches:
            maxitems = 0

        try:
            enquire = self.get_enquire()
            matches = enquire.get_mset(self.first, maxitems, check_at_least)
        except (xapian.NetworkError, xapian.DatabaseModifiedError):
            database = xapian_reopen(self.database, data=self.data, log=self.log)
            if self.database != database:
                self.database = database
            try:
                enquire = self.get_enquire()
                matches = enquire.get_mset(self.first, maxitems, check_at_least)
            except (xapian.NetworkError, xapian.DatabaseError) as exc:
                raise XapianError(exc)

        self.produced = 0
        self.estimated = None
        self.size = matches.size()
        if self.get_size:
            self.estimated = matches.get_matches_estimated()
            yield {
                'size': self.size,
                'estimated': self.estimated,
            }

        if self.spies:
            for name, spy in self.spies.items():
                self.dead or 'alive'  # Raises DeadException when needed
                for facet in spy.values():
                    self.dead or 'alive'  # Raises DeadException when needed
                    yield {
                        'facet': name,
                        'term': facet.term.decode('utf-8'),
                        'termfreq': facet.termfreq,
                    }

        produced = 0
        for match in matches:
            docid = match.docid
            self.database, document = get_document(self.database, docid, data=self.data, log=self.log)

            self.dead or 'alive'  # Raises DeadException when needed
            self.database, id = get_value(self.database, document, get_slot('id'), data=self.data, log=self.log)

            produced += 1
            result = {
                'id': id,
                'docid': docid,
                'rank': match.rank,
                'weight': match.weight,
                'percent': match.percent,
            }
            if self.get_data:
                self.database, data = get_data(self.database, document, data=self.data, log=self.log)
                if data is None:
                    continue
                try:
                    data = json.loads(data)
                except Exception:
                    data = base64.b64encode(data)
                result.update({
                    'data': data,
                })
            if self.get_terms:
                terms = []
                self.database, termlist = get_termlist(self.database, document, data=self.data, log=self.log)
                for t in termlist:
                    self.dead or 'alive'  # Raises DeadException when needed
                    terms.append(t.term.decode('utf-8'))
                result.update({
                    'terms': terms,
                })
            yield result
        self.produced = produced

    @property
    def results(self):
        return self.get_results()
