CouchDB Performance Characteristics
===================================

Python scripts to evaluate CouchDB performance.

couch_test.py
-------------
This is a general purpose script to help evaluate CouchDB performance on 
inserts and updates. There are 2 update modes, direct update or using 
the [Update Handler API](http://wiki.apache.org/couchdb/Document_Update_Handlers). 
You can also see how much improvements you'd get from using the
[_bulk_docs API](http://wiki.apache.org/couchdb/HTTP_Bulk_Document_API).

This script was helpful in illustrating the performance difference between 
inserts and updates. It also helped in showing how CouchDB scales according 
the following factors:

* Document size
* Number of JSON properties
* _bulk_docs API enabled

Usage:

    $ ./couch_test.py -db -I
    Testing with doc target size = "12 kB", # of properties = "10", inserts = "1000"
    each document is approximately 12 kB
    1000 inserts in 3.12067008018 seconds, 320.443998983 ops/second

update_test.py
--------------
This script evaluates the performance of different update methods under
more controlled conditions. It was necessary to create a separate script 
to benchmark update performance because the `couch_test.py` script 
depends on the [couchdb-python API](http://code.google.com/p/couchdb-python/), 
which does not support invoking Update Handlers.

This script was useful in illustrating the impact of network bandwidth 
and latency on CouchDB. CouchDB's architecture does not lend itself well
to making many small updates to a large document. It can be rectified by
using Update Handlers, but it may add complexity because your update 
logic now must be in JavaScript.

Usage:

    $ ./update_test.py -db --update1 -n 500 --numDocs 25
    Testing with doc target size = "12 kB", # of properties = "10", updates = "500", numDocs = "25"
    each document is approximately 12.1 kB
    Using direct update with _bulk_docs API
    500 updates in 4.00638318062 seconds, 124.800843419 ops/second
