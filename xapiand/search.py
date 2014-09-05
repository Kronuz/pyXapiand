from __future__ import unicode_literals, absolute_import

import base64
import logging

import xapian

from . import json
from .core import xapian_reopen, get_slot
from .serialise import normalize
from .exceptions import XapianError

MAX_DOCS = 10000


class Search(object):
    def __init__(self, database, obj,
                 get_matches=True, get_data=True, get_terms=False, get_size=False,
                 data='.', log=logging, dead=False):
        self.log = log
        self.data = data
        self.get_matches = get_matches
        self.get_terms = get_terms
        self.get_data = get_data
        self.get_size = get_size
        self.size = None
        self.dead = dead

        # SEARCH
        qp = xapian.QueryParser()
        qp.set_database(database)

        query = None

        # Build final query:
        search = obj.get('search')
        if search and search != '*':
            try:
                query = xapian.Query.unserialise(search)
            except xapian.InvalidArgumentError:
                pass
            if not query:
                search = normalize(search)
                try:
                    query = qp.parse_query(search)
                except (xapian.NetworkError, xapian.DatabaseModifiedError):
                    database = xapian_reopen(database, data=self.data, log=self.log)
                    qp.set_database(database)
                    query = qp.parse_query(search)

        partials = obj.get('partials')
        if partials:
            # Partials (for autocomplete) using FLAG_PARTIAL and OP_AND_MAYBE
            partials_query = None
            for partial in partials:
                self.dead or 'alive'  # Raises DeadException when needed
                partial = normalize(partial)
                flags = xapian.QueryParser.FLAG_WILDCARD | xapian.QueryParser.FLAG_PARTIAL
                try:
                    _partials_query = qp.parse_query(partial, flags)
                except (xapian.NetworkError, xapian.DatabaseModifiedError):
                    database = xapian_reopen(database, data=self.data, log=self.log)
                    qp.set_database(database)
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

        terms = obj.get('terms')
        if terms:
            # Partials (for autocomplete) using FLAG_BOOLEAN and OP_AND
            terms = normalize(terms)
            try:
                terms_query = qp.parse_query(terms, xapian.QueryParser.FLAG_BOOLEAN)
            except (xapian.NetworkError, xapian.DatabaseModifiedError):
                database = xapian_reopen(database, data=self.data, log=self.log)
                qp.set_database(database)
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

        self.database = database
        self.query = query
        self.facets = obj.get('facets')
        self.check_at_least = obj.get('check_at_least', MAX_DOCS)
        self.maxitems = obj.get('maxitems', MAX_DOCS)
        self.first = obj.get('first', 0)
        self.sort_by = obj.get('sort_by')
        self.sort_by_reversed = obj.get('sort_by_reversed')

    def get_enquire(self, database):
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

        self.spies = spies
        self.warnings = warnings

        return enquire

    def get_results(self):
        try:
            doccount = self.database.get_doccount()
        except (xapian.NetworkError, xapian.DatabaseModifiedError):
            self.database = xapian_reopen(self.database, data=self.data, log=self.log)
            doccount = self.database.get_doccount()

        maxitems = max(min(self.maxitems, doccount - self.first, MAX_DOCS), 0)
        check_at_least = max(min(self.check_at_least, doccount, MAX_DOCS), 0)

        if not self.get_matches:
            maxitems = 0

        try:
            enquire = self.get_enquire(self.database)
            matches = enquire.get_mset(self.first, maxitems, check_at_least)
        except (xapian.NetworkError, xapian.DatabaseModifiedError):
            self.database = xapian_reopen(self.database, data=self.data, log=self.log)
            try:
                enquire = self.get_enquire(self.database)
                matches = self.enquire.get_mset(self.first, maxitems, check_at_least)
            except (xapian.NetworkError, xapian.DatabaseError) as e:
                raise XapianError(e)

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
            self.dead or 'alive'  # Raises DeadException when needed
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
                    self.database = xapian_reopen(self.database, data=self.data, log=self.log)
                    try:
                        data = match.document.get_data()
                    except xapian.NetworkError as e:
                        raise XapianError(e)
                except xapian.DocNotFoundError:
                    continue
                try:
                    data = json.loads(data)
                except:
                    data = base64.b64encode(data)
                result.update({
                    'data': data,
                })
            if self.get_terms:
                terms = []
                for t in match.document.termlist():
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
