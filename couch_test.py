#!/usr/bin/env python
import couchdb
import sys
import time 
import copy
from optparse import OptionParser
from uuid import uuid4
import httplib2


"""
A script help gather data on the behaviors of CouchDB when it comes to inserts and updates.

Follow the instructions at http://wiki.apache.org/couchdb/Getting_started_with_Python to setup Python dependencies. Basically
From a terminal window:

$ wget http://peak.telecommunity.com/dist/ez_setup.py
$ sudo python ez_setup.py
$ wget http://pypi.python.org/packages/2.6/C/CouchDB/CouchDB-0.8-py2.6.egg
$ sudo easy_install CouchDB-0.8-py2.6.egg
"""

# Default program values
TESTDB='jc_couch_test'
DOC_SIZE=12 # Target size for the document. The actual size may vary.
NUM_DOC_PROPERTIES=10 # Number of JSON properties in the document.
NUM_TEST_OPS=1000 # Number of updates to perform, higher == better estimates
SERVER="http://localhost:5984" # Location of couchdb server
NUM_TEST_DOCS=100 # For update mode, populate the test db with this many documents to update
# End Default program values

parser = OptionParser()
parser.add_option('--update2', default=False, help='Runs the "Update" test using a design document', action='store_true')
parser.add_option('--update', '-U', default=False, help='Run the "Update" test.', action='store_true')
parser.add_option('--insert', '-I', default=False, help='Run the "Insert" test.', action='store_true')
parser.add_option('--numDocs', default=NUM_TEST_DOCS, type='int', help='For "Update" mode, the number of test docs to update.')
parser.add_option('--size', '-s', default=DOC_SIZE, type='int', help='Target document size in kb (approximate), a higher value equates to larger documents being.')
parser.add_option('--numProps', '-p', default=NUM_DOC_PROPERTIES, type='int', help='Number of properties in the JSON document object.')
parser.add_option('--numOps', '-n', default=NUM_TEST_OPS, type='int', help='Number of insert/updates to perform.')
parser.add_option('--testdb', default=TESTDB,  help='Name of the test database, defaults to "%s".' % TESTDB)
parser.add_option('--deleteDb', '-d', default=False, help='Delete the test database "'+TESTDB+'" if it already exists.', action='store_true')
parser.add_option('--batch', '-b', default=False, help='Use batch mode.', action='store_true')
parser.add_option('--server', default=SERVER, help='URL to the CouchDB server, defaults to "%s"'%SERVER)
parser.add_option('--compactDetails',default=False, help='Shows details about compaction at the end of the run', action='store_true')
params = parser.parse_args()[0]

# formatting number of bytes to human readable format
# from http://mail.python.org/pipermail/python-list/2008-August/1171178.html
_abbrevs = [
    (1<<50, ' PB'),
    (1<<40, ' TB'),
    (1<<30, ' GB'),
    (1<<20, ' MB'),
    (1<<10, ' kB'),
    (1, ' bytes')
    ]
def bytestr(size, precision=1):
    """Return a string representing the greek/metric suffix of a size"""
    if size==1:
        return '1 byte'
    for factor, suffix in _abbrevs:
        if size >= factor:
            break

    float_string_split = ('%s'%(size/float(factor))).split('.')
    integer_part = float_string_split[0]
    decimal_part = float_string_split[1]
    if int(decimal_part[0:precision]):
        float_string = '.'.join([integer_part, decimal_part[0:precision]])
    else:
        float_string = integer_part
    return float_string + suffix

propLen = (params.size*1024/params.numProps) - 16
# Create a document and insert it into the db
docId = 0
def mkdoc():
    global docId
    doc = {
        'upOrDown': 'up'
        }
    docId = docId + 1
    for i in range(0, params.numProps):
        doc[('key%s'%i).rjust(10,'0')] = 'x'.ljust(propLen)
    return doc

def flipFlag(doc):
    """Flip the 'upOrDown' flag of our test document."""
    if doc['upOrDown'] == 'up':
        doc['upOrDown'] = 'down'
    else:
        doc['upOrDown'] = 'up'

if not params.update2 and not params.insert and not params.update:
    print('Must specify --update2, -I, or -U to continue (update2, insert or update).')
    sys.exit(1)

couch = couchdb.Server(params.server)

if params.testdb in couch and not params.deleteDb:
    print('Note: A database named %s already exists' % params.testdb)

if params.testdb in couch and params.deleteDb:
    couch.delete(params.testdb)

if not params.testdb in couch:
    couch.create(params.testdb)

db = couch[params.testdb]
if params.update2:
    print('Testing with doc target size = "%s kB", # of properties = "%s", updates = "%s", numDocs = "%s"' % (params.size, params.numProps, params.numOps, params.numDocs))
    docs = [] 
    doc = mkdoc()
    for i in range(0, min(params.numDocs, params.numOps)):
        docCopy = copy.deepcopy(doc)
        docId = uuid4().hex
        docCopy['_id'] = docId
        db.save(docCopy)
        docs.append(docId)
    db.commit()
    print('each document is approximately %s' % bytestr(len(repr(db.get(docs[0]).items()))))
    designDoc = """
{ "updates": {
        "increment": "function(doc, req) { 
                if ( typeof(doc.counter) !== 'undefined' ) {
                        doc.counter = doc.counter + 1; 
                } else {
                        doc.counter = 0
                }
                return [doc, \\"counter = \\" + doc.counter]; }"
        }
}
"""
    http = httplib2.Http('.cache')
    http.request('%s/%s/_design/%s' % (params.server, params.testdb, 'test_design'), 'PUT', designDoc)
    i = 0
    stime = time.time()
    for x in range(0, params.numOps):
        if i >= params.numDocs:
            i = 0
        http.request('%s/%s/_design/%s/_update/increment/%s' % (params.server, params.testdb, 'test_design', docs[i]), 'PUT', body='')
        i = i + 1
    etime = time.time()
    print('%s updates in %s seconds, %s ops/second' % (params.numOps, (etime-stime), params.numOps/(etime-stime)))
elif params.insert:
    print('Testing with doc target size = "%s kB", # of properties = "%s", inserts = "%s"' % (params.size, params.numProps, params.numOps))
    docOrig = mkdoc()
    print('each document is approximately %s' % bytestr(len(repr(docOrig))))
    stime = time.time()
    for x in range(0, params.numOps):
        docCopy = copy.deepcopy(docOrig)
        docCopy['_id'] = uuid4().hex
        if params.batch:
            db.save(docCopy, batch='ok')
        else:
            db.save(docCopy)
    etime = time.time()
    print('%s inserts in %s seconds, %s ops/second' % (params.numOps, (etime-stime), params.numOps/(etime-stime)))
elif params.update:
    print('Testing with doc target size = "%s kB", # of properties = "%s", updates = "%s", numDocs = "%s"' % (params.size, params.numProps, params.numOps, params.numDocs))
    docs = [] 
    doc = mkdoc()
    for i in range(0, min(params.numDocs, params.numOps)):
        docCopy = copy.deepcopy(doc)
        docId = uuid4().hex
        docCopy['_id'] = docId
        db.save(docCopy)
        docs.append(docId)
    db.commit()
    print('each document is approximately %s' % bytestr(len(repr(db.get(docs[0]).items()))))

    i = 0
    stime = time.time()
    for x in range(0, params.numOps):
        if i >= params.numDocs:
            i = 0
        doc = db.get(docs[i])
        flipFlag(doc)
        if params.batch:
            db.save(doc, batch='ok')
        else:
            db.save(doc)
        i = i + 1
    etime = time.time()
    print('%s updates in %s seconds, %s ops/second' % (params.numOps, (etime-stime), params.numOps/(etime-stime)))

db.commit()
if params.compactDetails:
    print('database size before compaction: %s' % bytestr(db.info()['disk_size']))
    db.compact()  
    long_compaction = False
    while db.info()['compact_running']:
        time.sleep(1)
        if db.info()['compact_running']:
            long_compaction = True
            sys.stdout.write('.')
    if long_compaction:
        sys.stdout.write('\n')
    print('database size after compaction: %s'% bytestr(db.info()['disk_size']))


