Xapiand
=======

Xapian indexing and querying server implemented in Python

Start server by running: `python xapiand/bin/worker.py`


Indexing protocol
-----------------

Connect to server using `nc localhost 8890`

Open a database: `using example`

Index a document: `index {"id": "doc1", "terms": [{"term": "test"}, {"term": "first"}], "data": "DATA"}`

Search for a document: `search test`


Indexing from Python
--------------------

Import client: `from xapiand import Xapian`

Connect: `x = Xapian('localhost:8890', using=['example'])`

Index a document: `x.index({"id": "doc2", "terms": [{"term": "test"}, {"term": "second"}], "data": "DATA"})`

Search for a document: `list(x.search('test'))`


License
======

Dual license: MIT and GNU GLP v2


Author
=====
Germán M. Bravo (Kronuz)
