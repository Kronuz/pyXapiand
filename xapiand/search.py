from __future__ import unicode_literals, absolute_import

import base64
from hashlib import md5

import xapian

from . import json
from .core import xapian_reopen, KEY_RE
from .parser import search_parser
from .serialise import normalize
from .exceptions import XapianError


class Search(object):
    def __init__(self, database, query_string, get_matches=True, get_data=True, get_terms=False, get_size=False, data='.', log=None):
        self.log = log
        self.data = data
        self.query_string = query_string
        self.get_matches = get_matches
        self.get_terms = get_terms
        self.get_data = get_data
        self.get_size = get_size
        self.size = None
        self.database = database

        doccount = database.get_doccount()

        # SEARCH
        qp = xapian.QueryParser()
        qp.set_database(database)

        parsed = search_parser(query_string)
        first, maxitems, sort_by, sort_by_reversed, spies, check_at_least, partials, terms, search = parsed

        maxitems = max(min(maxitems, doccount - first, 10000), 0)
        check_at_least = max(min(check_at_least, doccount, 10000), 0)

        # Build final query:
        query = None
        if search and search != '*':
            try:
                query = xapian.Query.unserialise(search)
            except xapian.InvalidArgumentError:
                pass
            if not query:
                search = normalize(search)
                try:
                    query = qp.parse_query(search)
                except xapian.DatabaseModifiedError:
                    xapian_reopen(database, data=self.data, log=self.log)
                    query = qp.parse_query(search)

        if partials:
            # Partials (for autocomplete) using FLAG_PARTIAL and OP_AND_MAYBE
            partials_query = None
            for partial in partials:
                partial = normalize(partial)
                flags = xapian.QueryParser.FLAG_WILDCARD | xapian.QueryParser.FLAG_PARTIAL
                try:
                    _partials_query = qp.parse_query(partial, flags)
                except xapian.DatabaseModifiedError:
                    xapian_reopen(database, data=self.data, log=self.log)
                    _partials_query = qp.parse_query(partial, flags)
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

        if terms:
            # Partials (for autocomplete) using FLAG_BOOLEAN and OP_AND
            terms = normalize(terms)
            try:
                terms_query = qp.parse_query(terms, xapian.QueryParser.FLAG_BOOLEAN)
            except xapian.DatabaseModifiedError:
                xapian_reopen(database, data=self.data, log=self.log)
                terms_query = qp.parse_query(terms, xapian.QueryParser.FLAG_BOOLEAN)
            if query:
                query = xapian.Query(
                    xapian.Query.OP_AND,
                    query,
                    terms_query,
                )
            else:
                query = terms_query

        if not query:
            if search == '*':
                query = xapian.Query('')
            else:
                query = xapian.Query()

        self.query = str(query)

        self.enquire = xapian.Enquire(database)
        # enquire.set_weighting_scheme(xapian.BoolWeight())
        # enquire.set_docid_order(xapian.Enquire.DONT_CARE)
        # if weighting_scheme:
        #     enquire.set_weighting_scheme(xapian.BM25Weight(*self.weighting_scheme))
        self.enquire.set_query(query)

        self.warnings = []
        if spies:
            _spies, spies = spies, {}
            for name in _spies:
                name = name.strip().lower()
                if KEY_RE.match(name):
                    slot = int(md5(name).hexdigest(), 16) & 0xffffffff
                    spy = xapian.ValueCountMatchSpy(slot)
                    self.enquire.add_matchspy(spy)
                    spies[name] = spy
                else:
                    self.warnings.append("Ignored document value name (%r)" % name)

        if sort_by:
            _sort_by, sort_by = sort_by, []
            for sort_field in _sort_by:
                if sort_field.startswith('-'):
                    reverse = True
                    sort_field = sort_field[1:]  # Strip the '-'
                else:
                    reverse = False
                sort_by.append((sort_field, reverse))

            sorter = xapian.MultiValueKeyMaker()
            for name, reverse in sort_by:
                name = name.strip().lower()
                if KEY_RE.match(name):
                    slot = int(md5(name).hexdigest(), 16) & 0xffffffff
                    sorter.add_value(slot, reverse)
                else:
                    self.warnings.append("Ignored document value name (%r)" % name)
            self.enquire.set_sort_by_key_then_relevance(sorter, sort_by_reversed)

        self.first = first
        self.maxitems = maxitems
        self.check_at_least = check_at_least
        self.spies = spies

    def get_results(self):
        if not self.get_matches:
            self.maxitems = 0

        try:
            matches = self.enquire.get_mset(self.first, self.maxitems, self.check_at_least)
        except (xapian.NetworkError, xapian.DatabaseModifiedError):
            xapian_reopen(self.database, data=self.data, log=self.log)
            try:
                matches = self.enquire.get_mset(self.first, self.maxitems, self.check_at_least)
            except xapian.NetworkError as e:
                raise XapianError(e)

        if self.get_size:
            self.size = matches.size()

        if self.spies:
            for name, spy in self.spies.items():
                for facet in spy.values():
                    yield json.dumps({
                        'facet': name,
                        'term': facet.term.decode('utf-8'),
                        'termfreq': facet.termfreq,
                    }, ensure_ascii=False)

        produced = 0
        for match in matches:
            produced += 1
            result = {
                'id': match.docid,
                'rank': match.rank,
                'weight': match.weight,
                'percent': match.percent,
            }
            if self.get_data:
                try:
                    data = match.document.get_data()
                except (xapian.NetworkError, xapian.DatabaseModifiedError):
                    xapian_reopen(self.database, data=self.data, log=self.log)
                    try:
                        data = match.document.get_data()
                    except xapian.NetworkError as e:
                        raise XapianError(e)
                try:
                    data = json.loads(data)
                except:
                    data = base64.b64encode(data)
                result.update({
                    'data': data,
                })
            if self.get_terms:
                result.update({
                    'terms': [t.term.decode('utf-8') for t in match.document.termlist()],
                })
            yield json.dumps(result, ensure_ascii=False)

        if self.size is None:
            self.size = produced

    @property
    def results(self):
        return self.get_results()
