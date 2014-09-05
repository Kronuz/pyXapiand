from __future__ import absolute_import, unicode_literals

import os
import hashlib

try:
    import cPickle as pickle
except ImportError:
    import pickle

from xapiand import Xapian
from xapiand.core import get_slot
from xapiand.serialise import LatLongCoord
from xapiand.exceptions import XapianError
from xapiand.results import XapianResults

from haystack import connections
from haystack.constants import ID, DEFAULT_ALIAS
from haystack.backends import BaseEngine, BaseSearchBackend, BaseSearchQuery, log_query
from haystack.models import SearchResult
from haystack.utils import get_identifier

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


DOCUMENT_ID_TERM_PREFIX = 'Q'
DOCUMENT_CUSTOM_TERM_PREFIX = 'X'
DOCUMENT_CT_TERM_PREFIX = DOCUMENT_CUSTOM_TERM_PREFIX + 'CT'


def consistent_hash(key, num_buckets):
    """
    A Fast, Minimal Memory, Consistent Hash Algorithm (Jump Consistent Hash)

    Hash accepts "a 64-bit key and the number of buckets. It outputs a number
    in the range [0, buckets]." - http://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf

    The C++ implementation they provide is as follows:

    int32_t JumpConsistentHash(uint64_t key, int32_t num_buckets) {
        int64_t b = -1, j = 0;
        while (j < num_buckets) {
            b   = j;
            key = key * 2862933555777941757ULL + 1;
            j   = (b + 1) * (double(1LL << 31) / double((key >> 33) + 1));
        }
        return b;
    }

    assert consistent_hash(1, 1) == 0
    assert consistent_hash(256, 1024) == 520
    assert consistent_hash(42, 57) == 43
    assert consistent_hash(0xDEAD10CC, -666) == 0
    assert consistent_hash(0xDEAD10CC, 1) == 0
    assert consistent_hash(0xDEAD10CC, 666) == 361

    """
    if not isinstance(key, (int, long)):
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        key = int(hashlib.md5(key).hexdigest(), 16) & 0xffffffffffffffff
    b, j = -1, 0
    if num_buckets < 0:
        num_buckets = 1
    while j < num_buckets:
        b = int(j)
        key = ((key * 2862933555777941757) + 1) & 0xffffffffffffffff
        j = float(b + 1) * (float(1 << 31) / float((key >> 33) + 1))
    return b & 0xffffffff


class XapianSearchResults(XapianResults):
    def get_data(self, result):
        app_label, module_name, pk, model_data = pickle.loads(result['data'].encode('utf-8'))
        return SearchResult(app_label, module_name, pk, result['weight'], **model_data)


class XapianSearchBackend(BaseSearchBackend):
    def __init__(self, connection_alias, language=None, **connection_options):
        super(XapianSearchBackend, self).__init__(connection_alias, **connection_options)

        self.endpoints = []
        self.timeout = connection_options.get('TIMEOUT', 0)

        if 'PATH' in connection_options:
            paths = connection_options['PATH']
            prefix = connection_options.get('PREFIX', '')
            if not isinstance(paths, (tuple, list)):
                paths = [paths]
            for path in paths:
                path = os.path.join(prefix, path)
                self.endpoints.append(path)

        if not self.endpoints:
            raise ImproperlyConfigured("You must specify 'PATH' in your settings for connection '%s'." % connection_alias)

        self.language = language or getattr(settings, 'HAYSTACK_XAPIAN_LANGUAGE', 'english')
        try:
            self.xapian = Xapian('localhost:8890', using=self.endpoints)
        except XapianError:
            self.xapian = Xapian('localhost:8890')
            for endpoint in self.endpoints:
                self.xapian.create(endpoint)
            self.xapian.using(self.endpoints)

    def updater(self, index, obj, commit):
        data = index.full_prepare(obj)
        weights = index.get_field_weights()

        document_values = {}
        document_terms = []
        document_texts = []
        for field in self.schema:
            field_name = field['field_name']
            if field_name in data:
                prefix = '%s%s' % (DOCUMENT_CUSTOM_TERM_PREFIX, get_slot(field_name))

                values = data[field_name]
                if not field['multi_valued']:
                    values = [values]

                if not field['stored']:
                    del data[field_name]

                try:
                    weight = int(weights[field_name])
                except KeyError:
                    weight = 1

                field_type = field['type']
                for value in values:
                    if not value:
                        continue

                    if field_type == 'text':
                        if field['mode'] == 'autocomplete':  # mode = content, autocomplete, tagged
                            document_terms.append(dict(term=value, weight=weight, prefix=DOCUMENT_CUSTOM_TERM_PREFIX + 'AC'))
                        elif field['mode'] == 'tagged':
                            document_terms.append(dict(term=value, weight=weight, prefix=prefix))

                    elif field_type in ('ngram', 'edge_ngram'):
                        NGRAM_MIN_LENGTH = 1
                        NGRAM_MAX_LENGTH = 15
                        terms = _ngram_terms({value: weight}, min_length=NGRAM_MIN_LENGTH, max_length=NGRAM_MAX_LENGTH, split=field_type == 'edge_ngram')
                        for term, weight in terms.items():
                            document_terms.append(dict(term=term, weight=weight, prefix=prefix))

                    elif field_type == 'geo_point':
                        lat, _, lng = value.partition(',')
                        value = LatLongCoord(float(lat), float(lng))
                        data[field_name] = value

                    if field_name == self.content_field_name:
                        document_texts.append(dict(text=value, weight=weight, prefix=prefix))

                    document_values[field_name] = value

        document_data = pickle.dumps((obj._meta.app_label, obj._meta.module_name, obj.pk, data))
        document_id = DOCUMENT_ID_TERM_PREFIX + get_identifier(obj)

        document_terms.append(dict(term='%s.%s' % (obj._meta.app_label, obj._meta.module_name), weight=0, prefix=DOCUMENT_CT_TERM_PREFIX))

        endpoint = self.endpoints[consistent_hash(document_id, len(self.endpoints))]

        self.xapian.index(
            id=document_id,
            data=document_data,
            terms=document_terms,
            values=document_values,
            texts=document_texts,
            endpoints=[endpoint],
            positions=True,
        )

    def update(self, index, iterable, commit=False, mod=False):
        for obj in iterable:
            self.updater(index, obj, commit=commit)

    def remove(self, obj, commit=False):
        pass

    def clear(self, models=[], commit=True):
        pass

    @log_query
    def search(self, query_string, **kwargs):
        """
        Returns:
            A dictionary with the following keys:
                `results` -- An iterator of `SearchResult`
                `hits` -- The total available results
                `facets` - A dictionary of facets with the following keys:
                    `fields` -- A list of field facets
                    `dates` -- A list of date facets
                    `queries` -- A list of query facets
            If faceting was not used, the `facets` key will not be present

        """

        facets = {
            'fields': {},
            'dates': {},
            'queries': {},
        }

        results = self.xapian.search(query_string, results_class=XapianSearchResults)
        for facet in results.facets:
            facets['fields'][facet['name']] = (facet['term'], facet['termfreq'])

        return {
            'results': results,
            'facet': facets,
            'hits': results.estimated,
            'size': results.size,
        }

    def build_search_kwargs(self, query_string, sort_by=None, start_offset=0, end_offset=None,
                            fields='', highlight=False, facets=None,
                            date_facets=None, query_facets=None,
                            narrow_queries=None, spelling_query=None,
                            within=None, dwithin=None, distance_point=None,
                            models=None, limit_to_registered_models=None,
                            result_class=None):
        pass

    def build_schema(self, fields):
        """
        Build the schema from fields.

        Required arguments:
            ``fields`` -- A list of fields in the index

        Returns a list of fields in dictionary format ready for inclusion in
        an indexed meta-data.
        """
        content_field_name = ''
        schema_fields = [
            {'field_name': ID, 'type': 'text', 'multi_valued': 'false', 'column': 0, 'stored': True, 'mode': None},
        ]
        column = len(schema_fields)

        for field_name, field_class in sorted(fields.items(), key=lambda n: n[0]):
            if field_class.document is True:
                content_field_name = field_class.index_fieldname

            if field_class.indexed is True:
                field_data = {
                    'field_name': field_class.index_fieldname,
                    'type': 'text',
                    'multi_valued': False,
                    'column': column,
                    'stored': field_class.stored,
                    'mode': field_class.mode,
                }

                if field_class.field_type in ['date', 'datetime']:
                    field_data['type'] = 'date'
                elif field_class.field_type == 'integer':
                    field_data['type'] = 'long'
                elif field_class.field_type == 'float':
                    field_data['type'] = 'float'
                elif field_class.field_type == 'boolean':
                    field_data['type'] = 'boolean'
                elif field_class.field_type == 'ngram':
                    field_data['type'] = 'ngram'
                elif field_class.field_type == 'edge_ngram':
                    field_data['type'] = 'edge_ngram'
                elif field_class.field_type == 'location':
                    field_data['type'] = 'geo_point'

                if field_class.is_multivalued:
                    field_data['multi_valued'] = True

                schema_fields.append(field_data)
                column += 1

        return (content_field_name, schema_fields)

    @property
    def schema(self):
        if not hasattr(self, '_schema'):
            fields = connections[self.connection_alias].get_unified_index().all_searchfields()
            self._content_field_name, self._schema = self.build_schema(fields)
        return self._schema

    @property
    def content_field_name(self):
        if not hasattr(self, '_content_field_name'):
            fields = connections[self.connection_alias].get_unified_index().all_searchfields()
            self._content_field_name, self._schema = self.build_schema(fields)
        return self._content_field_name


class XapianSearchQuery(BaseSearchQuery):
    def __init__(self, using=DEFAULT_ALIAS):
        super(XapianSearchQuery, self).__init__(using=using)
        # self._facets = []
        # self._terms = []
        # self._partial = []
        # self._search = []
        # self._offset = None
        # self._limit = None
        # self._order_by = []

    def build_query_fragment(self, field, filter_type, value):
        # import ipdb; ipdb.set_trace()
        if filter_type == 'contains':
            pass
            # self._search.append(value)
        elif filter_type == 'like':
            pass
        elif filter_type == 'exact':
            pass
        elif filter_type == 'gt':
            pass
        elif filter_type == 'gte':
            pass
        elif filter_type == 'lt':
            pass
        elif filter_type == 'lte':
            pass
        elif filter_type == 'startswith':
            pass
        elif filter_type == 'in':
            pass
        return value


class XapianEngine(BaseEngine):
    backend = XapianSearchBackend
    query = XapianSearchQuery


def _ngram_terms(terms, min_length=1, min_length_percentage=0, max_length=20, split=True):
    """
        :param terms: dictionary of (term, initial weight)
        :param min_length: Minimum ngram length
        :param max_length: Maximum ngram length
        :param min_length_percentage: Minimum length in percentage of the original term
    """
    split_terms = {}
    if not isinstance(terms, dict):
        terms = {terms: 1}

    for term, weight in terms.items():
        if term is not None:
            if split:
                _terms = term.split()
            else:
                _terms = [term]
            for _term in _terms:
                split_terms[_term] = max(split_terms.get(_term, 0), weight)

    # Find all the substrings of the term (all digit terms treated differently):
    final_terms = {}
    for term, weight in split_terms.items():
        term_len = len(term)
        for i in range(term_len):
            for j in range(i + 1, term_len + 1):
                l = j - i
                if l <= max_length and l >= min_length and l >= int(term_len * min_length_percentage):
                    _term = term[i:j]
                    _weight = int(float(weight * l) / term_len)
                    final_terms[_term] = max(final_terms.get(_term, 0), _weight)

    return final_terms
