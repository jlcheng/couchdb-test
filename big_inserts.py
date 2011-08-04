#!/usr/bin/env python
import couchdb
import sys
import time
import copy
from optparse import OptionParser

TESTDB='update_large_doc'
DOC_SIZE=12 # creates this number of fake properties, higher == larger doc
DOC_NUM_PROPERTIES=10 # Number of JSON properties
INSERTS=2000 # number of inserts to perform, higher == better estimates
SERVER_NAME="localhost"
SERVER_PORT=5984

parser = OptionParser()
parser.add_option('--size', '-s', help='Target document size in kb (approximate), a higher value equates to larger documents being inserted.')
parser.add_option('--numProps', '-p', help='Number of properties in the JSON document object.')
parser.add_option('--inserts', '-n', help='Total number of inserts to perform.')
parser.add_option('--deleteExisting', '-d', help='Deletes the test database "update_large_doc" if it already exists.', action='store_true')
parser.defaults = {
    'size': DOC_SIZE,
    'numProps': DOC_NUM_PROPERTIES,
    'inserts': INSERTS,
    'deleteExisting': False
}
params = parser.parse_args()[0]
params.numProps = int(params.numProps)
params.size = int(params.size)
params.inserts = int(params.inserts)


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

# If your CouchDB server is running elsewhere, set it up like this:
couch = couchdb.Server('http://%s:%s/' % (SERVER_NAME, SERVER_PORT))

if not TESTDB in couch:
    couch.create(TESTDB)
elif params.deleteExisting:
    couch.delete(TESTDB)
    couch.create(TESTDB)

db = couch[TESTDB]

print('testing with doc target size = "%s kB", # of properties = "%s", inserts = "%s"' % (params.size, params.numProps, params.inserts))

doc = mkdoc()
db.save(doc)
print('each document is approximately %s bytes' % bytestr(len(repr(db.get(doc['_id']).items()))))
doc = mkdoc()
stime = time.time()
for i in range(0, params.inserts):
    docCopy = copy.deepcopy(doc)
    db.save(docCopy, batch='ok')
etime = time.time()
db.commit()
print('%s inserts in %s seconds, %s inserts/second' % (params.inserts, (etime-stime), params.inserts/(etime-stime)))
db.compact()
print('database size before compaction: %s' % bytestr(db.info()['disk_size']))
while db.info()['compact_running']:
    time.sleep(1)
db.commit()
print('database size after compaction: %s' % bytestr(db.info()['disk_size']))


