CouchDB Performance Characteristics
===================================

Scala test case to evaluate CouchDB performance.

InsertTest
-------------
Prototype.

Note - due to the way sbt works, you need to surround "run" and its arguments with quotes.

Usage:

    $ ./sbt "run --delete -m insert -n 100"
    [info] Set current project to default-7c18be (in build file:/home/jcheng/work/demo/couchdb-test/scala/)
    [info] Running jcheng.InsertTest --delete -m insert -n 100
    Database 'jc_couch_test' deleted and recreated.
    12686
    4069 ms, 24.576063
    [success] Total time: 5 s, completed Aug 16, 2011 9:27:55 AM

