#!/usr/bin/env python
import couchdb
import sys
import time 
from optparse import OptionParser

TESTDB='update_large_doc'
DOC_SIZE=12 # creates this number of fake properties, higher == larger doc
DOC_NUM_PROPERTIES=10 # Number of JSON properties
UPDATES=2000 # number of updates to perform, higher == better estimates
SERVER_NAME="localhost"
SERVER_PORT=5984
NUM_TEST_DOCS=200

parser = OptionParser()
parser.add_option('--numDocs', help='Number of test docs to put into database.')
parser.add_option('--size', '-s', help='Target document size in kb (approximate), a higher value equates to larger documents being updated.')
parser.add_option('--numProps', '-p', help='Number of properties in the JSON document object.')
parser.add_option('--updates', '-n', help='Total number of updates to perform.')
parser.add_option('--deleteExisting', '-d', help='Deletes the test database "update_large_doc" if it already exists.', action='store_true')
parser.add_option('--batch', '-b', help='Uses batch mode', action='store_true')
parser.defaults = {
    'numDocs': NUM_TEST_DOCS,
    'size': DOC_SIZE,
    'numProps': DOC_NUM_PROPERTIES,
    'updates': UPDATES,
    'deleteExisting': False,
    'batch':False
}
params = parser.parse_args()[0]
params.numDocs = int(params.numDocs)
params.numProps = int(params.numProps)
params.size = int(params.size)
params.updates = int(params.updates)

# formatting number of bytes to human readable format
# from http://mail.python.org/pipermail/python-list/2008-August/1171178.html
_abbrevs = [
    (1<<50L, ' PB'),
    (1<<40L, ' TB'),
    (1<<30L, ' GB'),
    (1<<20L, ' MB'),
    (1<<10L, ' kB'),
    (1, ' bytes')
    ]
def bytestr(size, precision=1):
    """Return a string representing the greek/metric suffix of a size"""
    if size==1:
        return '1 byte'
    for factor, suffix in _abbrevs:
        if size >= factor:
            break

    float_string_split = `size/float(factor)`.split('.')
    integer_part = float_string_split[0]
    decimal_part = float_string_split[1]
    if int(decimal_part[0:precision]):
        float_string = '.'.join([integer_part, decimal_part[0:precision]])
    else:
        float_string = integer_part
    return float_string + suffix

propLen = (params.size*1024/params.numProps) - 16
# Create a document and insert it into the db
def mkdoc():
    doc = {
        'upOrDown': 'up'
        }
    for i in range(0, params.numProps):
        doc[('key%s'%i).rjust(10,'0')] = 'x'.ljust(propLen)
    return doc

def flipFlag(doc):
    """Flip the 'upOrDown' flag of our test document."""
    if doc['upOrDown'] == 'up':
        doc['upOrDown'] = 'down'
    else:
        doc['upOrDown'] = 'up'

# If your CouchDB server is running elsewhere, set it up like this:
couch = couchdb.Server('http://%s:%s/' % (SERVER_NAME, SERVER_PORT))

if not TESTDB in couch:
    couch.create(TESTDB)
elif params.deleteExisting:
    couch.delete(TESTDB)
    couch.create(TESTDB)

db = couch[TESTDB]

print('testing with doc target size = "%s kB", # of properties = "%s", updates = "%s", numDocs = "%s"' % (params.size, params.numProps, params.updates, params.numDocs))

docs = []
for i in range(0, min(params.numDocs, params.updates)):
    doc = mkdoc()
    db.save(doc)
    docs.append(doc)
db.commit()
print('each document is approximately %s bytes' % bytestr(len(repr(db.get(docs[i]['_id']).items()))))

i = 0
stime = time.time()
for x in range(0, params.updates):
    if i >= params.numDocs:
        i = 0
    docCopy = db.get(docs[i]['_id'])
    flipFlag(docCopy)
    if params.batch:
        db.save(doc, batch='ok')
    else:
        db.save(doc)
    i = i + 1
etime = time.time()
db.commit()
print('%s updates in %s seconds, %s updates/second' % (params.updates, (etime-stime), params.updates/(etime-stime)))
db.compact()
print('database size before compaction: %s' % bytestr(db.info()['disk_size']))
while db.info()['compact_running']:
    time.sleep(1)
db.commit()
print('database size after compaction: %s' % bytestr(db.info()['disk_size']))


