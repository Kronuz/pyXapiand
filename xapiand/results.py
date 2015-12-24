class XapianResult(object):
    pass


class XapianResults(object):
    def __init__(self, results):
        self.results = results

        self.size = 0
        self.estimated = 0
        self._first_result = None

        facets = []
        for result in self.results:
            if 'docid' in result:
                self._first_result = result
                break
            elif 'facet' in result:
                facets.append(result)
            elif 'size' in result:
                self.size = result['size']
                self.estimated = result['estimated']
        self.facets = facets

    def __len__(self):
        return self.size

    def __iter__(self):
        return self

    def next(self):
        if self._first_result is not None:
            _first_result, self._first_result = self._first_result, None
            return self.get_data(_first_result)
        return self.get_data(self.results.next())
    __next__ = next

    def get_data(self, result):
        return result
