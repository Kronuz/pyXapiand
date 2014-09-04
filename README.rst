=======
Xapiand
=======

Xapian indexing and querying server implemented in Python.

Start server by running: ``python xapiand/bin/worker.py``. this will start in
the console mode ``--detach`` is available as well as other common parameters.
See more information using ``--help``.

The server runs running on port 8890 by default.


Getting Started
===============

Indexing directly using the xapiand protocol
--------------------------------------------

Connect to server using ``nc localhost 8890`` or ``telent localhost 8890``

Open a database: ``USING example``

Index a document: ``INDEX {"id": "doc1", "terms": [{"term": "test"}, {"term": "first"}], "data": "DATA"}``

Search for a document: ``SEARCH test``

More commands are availabe using: ``HELP`` and ``HELP <command>``.


Indexing from the Python client
-------------------------------

Import client: ``from xapiand import Xapian``

Connect: ``x = Xapian('localhost:8890')``

Create a new database: ``x.create('example')``

Index a couple documents::

  x.index({
    "id": "doc2",
    "data": "DATA",
    "terms": [{"term": "test"}, {"term": "second"}],
    "values": {
      "date": datetime.datetime.now(),
      "color": "blue",
      "size": 2
    }
  })

  x.index({
      "id": "doc3",
      "data": "DATA",
      "terms": [{"term": "test"}, {"term": "third"}],
      "values": {
          "date": datetime.datetime.now(),
          "color": "red",
          "size": 1.4
      }
  })


Search for a document: ``list(x.search('test'))``


Indexing
========

Format of the JSON object to index (only "id" and "data" are required)::

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


Searching
=========

The query can have any or a mix of::

  SEARCH query_string
  PARTIAL <partial ...> [PARTIAL <partial ...>]...
  TERMS <term ...>
  FACETS <min> <field_name ...>
  OFFSET <offset>
  LIMIT <limit>
  ORDER BY <field_name ...> [ASC|DESC]


PARTIAL
-------

Partial is used to find documents in the way needed for autocomplete-like
searches. If multiple PARTIAL keywords are given, it finds documents containing
the first one AND MAYBE the second ones. For example, to find documents that
contain (``spider`` AND ``arac*``) AND MAYBE (``america``), you'd do something like:
``SEARCH PARTIAL spider arac PARTIAL america``


TERMS
-----

You can query for exact terms using ``TERMS <term>``. This will find docuemnts
that were indexed using those exact terms.

FACETS
------

Along the results, it returns facets for any number of given fields (fields must
have been indexed as values).


Remote Databases
================

Databases running with the ``xapian-tcpsrv`` can be used by opening them as:
``USING xapian://hostname.server:33333`` (33333 is the default, so
``xapian://hostname.server`` is equivalent).


Multiple Databases
==================

Clients can connect to multiple endpoints (databases) listing all the endpoints
as part of the ``USING`` command. types can be mixed: e.g.:
``USING xapian://hostname.server:33333 example``


Requirements
============

Xapian python bindings::

  $ sudo apt-get install python-xapian
  $ sudo apt-get install libxapian-dev

Also, the module uses gevent, install using::

  $ pip install gevent


License
=======

Dual license: MIT and GNU GLP v2


Author
======
Germán M. Bravo (Kronuz)
