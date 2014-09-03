Xapiand
=======

Xapian indexing and querying server implemented in Python

Start server by running: `python xapiand/bin/worker.py`. this will start in
the console mode `--detach` is available as well as other common parameters.
See more information using `--help`.

The server runs running on port 8890 by default.


Indexing directly using the xapiand protocol
--------------------------------------------

Connect to server using `nc localhost 8890`

Open a database: `using example`

Index a document: `index {"id": "doc1", "terms": [{"term": "test"}, {"term": "first"}], "data": "DATA"}`

Search for a document: `search test`

More commands are availabe using: `help` and `help <command>`.


Indexing from the Python client
-------------------------------

Import client: `from xapiand import Xapian`

Connect: `x = Xapian('localhost:8890', using=['example'])`

Index a document: `x.index({"id": "doc2", "terms": [{"term": "test"}, {"term": "second"}], "data": "DATA"})`

Search for a document: `list(x.search('test'))`


Indexing
========

Format of the JSON object to index (only "id" and "data" are required):

```
    {
        "id": "<id>",
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
```

Searching
=========

Format of the queries:

```
    SEARCH [query_string]
        [PARTIAL <partial ...>] ...
        [TERMS <term ...>]
        [FACETS <min> <field_name ...>] ...
        [OFFSET <offset>]
        [LIMIT <limit>]
        [ORDER BY <field_name ...> [ASC|DESC]]
```


PARTIAL
-------

Partial is used to find documents in the way needed for autocomplete-like
searches. If multiple PARTIAL keywords are given, it finds documents containing
the first one AND MAYBE the second ones. For example, to find documents that
contain (`spider` AND `arac*`) AND MAYBE (`america`), you'd do something like:
`SEARCH PARTIAL spider arac PARTIAL america`


TERMS
-----

You can query for exact terms using `TERMS <term>`. This will find docuemnts
that were indexed using those exact terms.

FACETS
------

Along the results, it returns facets for any number of given fields (fields must
have been indexed as values).


Requirements
============

The module uses gevent and dateutil, install using `pip install gevent` and
`pip install dateutil`.


License
=======

Dual license: MIT and GNU GLP v2


Author
======
Germán M. Bravo (Kronuz)
