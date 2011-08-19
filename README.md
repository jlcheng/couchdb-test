CouchDB Performance Characteristics
===================================

There are two implementation of test suites here. First, a Python implementation that was done very quickly. Second, a Scala implementation that has been really optimized to get the most out of CouchDB. 

There are several problems with the Python implementation, first and foremost, I am not a Python hacker and my ability to optimize Python code is very limited. Second, CouchDB appears to behave best with highly concurrent access. The underlying library used by couchdb-python does not appear to send requests in a truly concurrent fashion, and this limits the throughput. Contrasting with Python, I was able to use the [HttpAsyncClient](http://hc.apache.org/httpcomponents-asyncclient-dev/index.html) library in Scala to get exteremely good performance.

The Scala implementation has been very finely tuned and I'm seeing the CPU and IO being pushed to the limit on my machine during testing. It is probably a good test for CouchDB performance on your network. It is probably also a good benchmark to detect performance variation between different versions of CouchDB. It can also be a good example of how to test REST-based services.

* [Python](/tree/master/python)
* [Scala](/tree/master/scala)
