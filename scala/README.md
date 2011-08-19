CouchDB Performance Characteristics
===================================

Scala test case to evaluate CouchDB performance.

CouchDBTest `--mode=insert`
---------------------------
Inserts documents into CouchDB using the batch=ok API (note, the _bulk_docs API is not used).

In the following example, you can disable batch mode by omitting the '-b' option.

### Usage:

    $ ./sbt "run -b --delete -m update-direct -n 500 --size=200 --docCount=6"
    [info] Set current project to default-7c18be (in build file:/home/jcheng/work/demo/couchdb-test/scala/)
    [info] Running jcheng.CouchDBTest -b --delete -m update-direct -n 500 --size=200 --docCount=6
    Database 'jc_couch_test' deleted and recreated.
    Performing 500 ops (update-direct) with a document size of 200
    Created 6 test documents
    Conflict rate: 28/500, 5.60%
    500 ops in 10.86 s, 46.04 ops/sec
    [success] Total time: 12 s, completed Aug 19, 2011 12:24:41 PM

CouchDBTest `--mode=update-direct`
---------------------------------
Update documents in CouchDB by using a `GET` method, modify the returned doument, the `PUT` the modified document back. On write conflicts, the test will `GET` a new copy and try again.

It is not possible to run this test in batch mode.

### Usage:

    $./sbt "run -b --delete -m update-direct -n 500 --size=200 --docCount=6"
    [info] Set current project to default-7c18be (in build file:/home/jcheng/work/demo/couchdb-test/scala/)
    [info] Running jcheng.CouchDBTest -b --delete -m update-direct -n 500 --size=200 --docCount=6
    Database 'jc_couch_test' deleted and recreated.
    Performing 500 ops (update-direct) with a document size of 200
    Created 6 test documents
    Conflict rate: 107/500, 21.40%
    500 ops in 11.43 s, 43.76 ops/sec
    [success] Total time: 13 s, completed Aug 19, 2011 12:45:45 PM


CouchDBTest `--mode=update-handler`
----------------------------------
Update documents in CouchDB by using the [Document Update Handlers](http://wiki.apache.org/couchdb/Document_Update_Handlers) API.

In the following example, you can disable batch mode by omitting the '-b' option.

Because updates are managed by CouhDB, there will be no update conflicts. Unlike the `update-direct` test, the `--docCount` parameter does not impact performance.

### Usage:

    $ ./sbt "run -b --delete -m update-handler -n 500 --size=200 --docCount=6"
    [info] Set current project to default-7c18be (in build file:/home/jcheng/work/demo/couchdb-test/scala/)
    [info] Running jcheng.CouchDBTest -b --delete -m update-handler -n 500 --size=200 --docCount=6
    Database 'jc_couch_test' deleted and recreated.
    Performing 500 ops (update-handler) with a document size of 200
    Created 6 test documents
    Uploaded design document
    500 ops in 13.38 s, 37.36 ops/sec
    [success] Total time: 15 s, completed Aug 19, 2011 12:25:25 PM


