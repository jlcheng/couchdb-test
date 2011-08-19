/**
 * Copyright John Cheng, Aug 12, 2011
 */

package jcheng
import java.util.Arrays.asList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.Arrays

import scala.actors.Actor
import scala.io.Source

import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.entity.StringEntity
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient
import org.apache.http.nio.concurrent.FutureCallback
import org.apache.http.params.CoreConnectionPNames
import org.apache.http.util.EntityUtils
import org.apache.http.HttpResponse
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ObjectNode

import joptsimple.OptionParser
import joptsimple.OptionSet

object CouchDBTest {

  // A Jackson ObjectMapper to parse JSON
  val objectMapper = new ObjectMapper()

  // Create an HttpAsyncClient 
  // Define convenience methods:
  //   'request'            - synchronous GET/PUT/POST, return a status code and the response
  //   'flush'              - calls _ensure_full_commit API to flush change to disk
  //   'runCompactAndBlock' - waits for the database to finish compacting
  val httpAsyncClient = new DefaultHttpAsyncClient();
  val httpParams = httpAsyncClient.getParams()
  httpParams.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
  httpParams.setParameter(CoreConnectionPNames.TCP_NODELAY, true)
  httpParams.setParameter(CoreConnectionPNames.SO_REUSEADDR, true)
  def request(method: HttpUriRequest): (Int, String) = {
    try {
      val future = httpAsyncClient.execute(method, null)
      val response = future.get()
      val statusCode = response.getStatusLine().getStatusCode()
      val content = Source.fromInputStream(response.getEntity().getContent()).getLines.mkString
      return (statusCode, content)
    } catch {
      case e: Exception => return (-1, e.getMessage())
    }
  }
  def flush(dbUrl: String) = {
    val post = new HttpPost(dbUrl + "/_ensure_full_commit")
    post.setHeader("Content-Type", "application/json")
    request(post)
  }
  def runCompactAndBlock(dbUrl: String) = {
    flush(dbUrl)
    val post = new HttpPost(dbUrl + "/_compact")
    post.setHeader("Content-Type", "application/json")
    var isRunning = true
    while (isRunning) {
      var response = request(new HttpGet(dbUrl))
      val jsonObject = objectMapper.readTree(response._2)
      isRunning = jsonObject.path("compact_running").getBooleanValue()
    }
  }

  def main(argv: Array[String]): Unit = {
    // Surround everything in try/catch 
    //   1. So we can shut down httpAsyncClient when we're done
    //   2. Because sbt doesn't print stacktrace to stdout on error    
    try {
      httpAsyncClient.start()

      // Setup options parser
      val parser = new OptionParser()
      parser.acceptsAll(asList("help", "h"), "help")
      def printHelp() = parser.printHelpOn(System.out)
      val countSpec = parser.acceptsAll(asList("count", "n"), "Number of operations to perform.")
        .withOptionalArg()
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(1000)
      val serverSpec = parser.accepts("serverName", "Server name.")
        .withOptionalArg()
        .ofType(classOf[String])
        .defaultsTo("http://localhost:5984")
      val modeSpec = parser.acceptsAll(asList("mode", "m"),
        "test mode: one of insert, update-direct, or update-handler")
        .withOptionalArg()
        .ofType(classOf[java.lang.String])
      val sizeSpec = parser.acceptsAll(asList("size", "s"), "Size of JSON document in kB.")
        .withOptionalArg()
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(12)
      parser.acceptsAll(asList("delete", "D"), "Delete test database before test.")
      val dbNameSpec = parser.accepts("testdb", "Name of the test database")
        .withOptionalArg()
        .defaultsTo("jc_couch_test")
      val docCountSpec = parser.accepts("docCount", "For updates, the number of JSON documents in the database")
        .withOptionalArg()
        .ofType(classOf[java.lang.Integer])
        .defaultsTo(100)
      parser.acceptsAll(asList("batch", "b"), "Enable _bulk_docs API when possible.")

      // Validate options
      var options: OptionSet = null;
      try {
        options = parser.parse(argv: _*)
        if (options.has("h")) {
          parser.printHelpOn(System.out)
          return
        }
        if (!options.has("m") ||
          !List("insert", "update-direct", "update-handler").contains(modeSpec.value(options))) {
          println("test mode required: 'insert', 'update-direct', or 'update-handler'")
          return
        }
        if ("update-direct".equals(modeSpec.values(options)) && options.has("b")) {
          println("batch=ok is not compatible with consistent update-direct")
          return
        }
      } catch {
        // Exception is thrown by joptsimple when an undefined option is encountered
        case e: Exception =>
          println(e.getMessage())
          parser.printHelpOn(System.out)
          return
      }
      val serverName = serverSpec.value(options)
      val dbName = dbNameSpec.value(options)
      val dbUrl = serverName + "/" + dbName
      val count = countSpec.value(options)
      val docCount = docCountSpec.value(options)
      val mode = modeSpec.value(options)
      val size = sizeSpec.value(options)
      val doDelete = options.has("D")
      val doBatch = options.has("b")

      // Set up the test database
      var response = request(new HttpGet(serverName))
      if (response._1 != 200) {
        println("CouchDB appears to be down: " + response._2)
        return
      }
      response = request(new HttpGet(dbUrl))
      if (response._1 == 200) {
        if (doDelete) {
          response = request(new HttpDelete(dbUrl))
          response = request(new HttpPut(dbUrl))
          println("Database '%s' deleted and recreated.".format(dbName))
        } else {
          println("WARN: database '%s' already exists.".format(dbName))
        }
      } else if (response._1 == 404) {
        response = request(new HttpPut(dbUrl))
        println("Database '%s' created.".format(dbName))
      }
      runCompactAndBlock(dbUrl)

      // A reusable method for inserting documents into the database
      def insertDocs(insertCount: Int, batch: Boolean = false) {
        val docIds = 0 until insertCount
        val doc = mkdoc(size)
        val latch = new CountDownLatch(docIds.length)
        docIds.foreach { docId =>
          val put = new HttpPut(dbUrl + "/" + docId + (if (batch) "?batch=ok" else ""))
          put.setEntity(new StringEntity(doc))
          httpAsyncClient.execute(put, new FutureCallback[HttpResponse]() {
            override def completed(response: HttpResponse) {
              EntityUtils.consume(response.getEntity())
              latch.countDown()
            }
            override def failed(exception: Exception) {
              latch.countDown()
            }
            override def cancelled() {
              latch.countDown()
            }
          })
        }
        latch.await()
        runCompactAndBlock(dbUrl)
      }
      println("Performing %d ops (%s) with a document size of %d".format(count, mode, size))

      val sw = new StopWatch()
      mode match {
        case "insert" =>
          sw.zero()
          insertDocs(count, doBatch)
          sw.stop()
        case "update-direct" =>
          insertDocs(docCount)
          println("Created " + docCount + " test documents")

          val latch = new CountDownLatch(count)
          val updateDispatcher = new UpdateDirectDispatcher(dbUrl, latch)
          updateDispatcher.start()
          sw.zero()
          0.until(count).foreach { idx =>
            val docId = idx % docCount
            // Tell update dispatcher we need to update document with this docId,
            // dispatcher will retry if necessary!
            updateDispatcher ! docId
          }
          latch.await()
          updateDispatcher ! "stop"
          runCompactAndBlock(dbUrl)
          sw.stop()
          println("Conflict rate: %d/%d, %.2f%%".format(updateDispatcher.conflicts, count, (updateDispatcher.conflicts.toFloat / count.toFloat * 100)))
        case "update-handler" =>
          insertDocs(docCount)
          println("Created " + docCount + " test documents")

          val designDoc = "{ \"updates\": {                                                   \n" +
            "\"increment\": \"function(doc, req) {                                              " +
            "                 if ( typeof(doc.count) !== 'undefined' ) {                        " +
            "                    doc.count = doc.count + 1;                                     " +
            "                  } else {                                                         " +
            "                    doc.count = 0                                                  " +
            "                  }                                                                " +
            "                  return [doc, \\\"\\\"+doc._id +\\\",\\\" + doc.count+\\\"\\\"];  " +
            "                } \"                                                               " +
            "  }                                                                              \n" +
            "}                                                                                \n"
          val put = new HttpPut(dbUrl + "/_design/test_design")
          put.setEntity(new StringEntity(designDoc))
          request(put)
          println("Uploaded design document")

          val latch = new CountDownLatch(count)
          val incrementUrl = dbUrl + "/_design/test_design/_update/increment/"
          sw.zero()
          0.until(count).foreach { idx =>
            val docId = idx % docCount
            val put = new HttpPut(incrementUrl + docId + (if (doBatch) "?batch=ok" else ""))
            httpAsyncClient.execute(put, new FutureCallback[HttpResponse]() {
              override def completed(response: HttpResponse) {
                latch.countDown()
              }
              override def failed(exception: Exception) {
                latch.countDown()
              }
              override def cancelled() {
                latch.countDown()
              }
            })
          }
          latch.await()
          flush(dbUrl)
          sw.stop()
        case _ => {}
      }
      val opsPerSec = if (sw.elapsed < 1) 0f else (count.toFloat / sw.elapsed) * 1000f
      println("%d ops in %s, %,.2f ops/sec".format(
        count, sw.elapsedString, opsPerSec))
    } catch {
      case e: Throwable =>
        e.printStackTrace()
      case _ =>
        {}
    } finally {
      httpAsyncClient.shutdown()
    }
  }

  def mkdoc(size: Int): String = {
    val sb = new StringBuilder("{ \"count\":0, \"prop0\":\"")
    val buf = new Array[Char](size * 1024)
    Arrays.fill(buf, '0')
    sb.appendAll(buf)
    sb.append("\"}")
    return sb.toString()
  }

}

class UpdateDirectDispatcher(
  val dbUrl: String,
  val latch: CountDownLatch) extends Actor {

  val httpClient = CouchDBTest.httpAsyncClient;
  val objectMapper = CouchDBTest.objectMapper;
  var conflicts: Int = 0
  val docUrlCache = new ConcurrentHashMap[Int, String]()

  def getDocUrl(docId: Int): String = {
    if (docUrlCache.contains(docId)) {
      return docUrlCache.get(docId)
    }
    val docUrl = dbUrl + "/" + docId
    docUrlCache.putIfAbsent(docId, docUrl)
    return docUrl
  }

  def act() {
    loop {
      react {
        case docId: Int =>
          val docUrl = dbUrl + "/" + docId
          val entity = httpClient.execute(new HttpGet(docUrl), null).get().getEntity()
          val docJson = objectMapper.readTree(entity.getContent()).asInstanceOf[ObjectNode]
          val count = docJson.path("count").getIntValue()
          docJson.put("count", count + 1)
          val put = new HttpPut(docUrl)
          put.setEntity(new StringEntity(objectMapper.writeValueAsString(docJson)))
          httpClient.execute(put, new FutureCallback[HttpResponse]() {
            override def completed(response: HttpResponse) {
              if (response.getStatusLine().getStatusCode() == 201) {
                latch.countDown()
              } else {
                // Record # of conflicts and tell the dispatcher to process this docId again
                conflicts = conflicts + 1
                UpdateDirectDispatcher.this ! docId
              }
            }
            override def failed(exception: Exception) {
              latch.countDown()
            }
            override def cancelled() {
              latch.countDown()
            }
          })
        case "stop" =>
          exit()
      }
    }
  }
}

/**
 * Abstraction for a CouchDB Update command.
 *
 * <p>An CouchDB update attempt may fail (revision out of date) and may need to be retried.
 * This abstraction captures the state and logic necessary to an update attempt so that it can
 * be queued and retried.</p>
 */
class RepeatableUpdateCommand(val docUrl: String) {

}

class StopWatch(var stime: Long = 0) {
  var etime = stime
  var elapsed: Long = 0

  def zero(): StopWatch = {
    stime = System.currentTimeMillis()
    etime = stime
    return this
  }

  def stop(): StopWatch = {
    etime = System.currentTimeMillis()
    elapsed = etime - stime
    return this
  }

  def elapsedString: String = {
    if (elapsed > 10000) {
      return "%,.2f s".format(elapsed.toFloat / 1000f)
    } else {
      return "%d ms".format(elapsed)
    }
  }
}
