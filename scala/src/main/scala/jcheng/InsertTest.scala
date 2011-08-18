/**
 * Copyright John Cheng, Aug 12, 2011
 */

/**
 * @author jcheng
 *
 */
package jcheng
import java.util.Arrays.asList
import java.util.concurrent.CountDownLatch
import java.util.Arrays

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

object InsertTest {

  def main(argv: Array[String]): Unit = {
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
    } catch {
      // Exception is thrown by joptsimple when an undefined option is encountered
      case e: Exception =>
        println(e.getMessage())
        parser.printHelpOn(System.out)
        return
    }
    val serverName = serverSpec.value(options)
    val dbName = dbNameSpec.value(options)
    val dbUrl = "%s/%s".format(serverName, dbName)
    val count = countSpec.value(options)
    val docCount = docCountSpec.value(options)
    val mode = modeSpec.value(options)
    val size = sizeSpec.value(options)
    val doDelete = options.has("D")
    val doBatch = options.has("b")

    // Create an HttpAsyncClient 
    // Define convenience method:
    //   'request' - synchronous GET/PUT/POST, return a status code and the response
    //   'flush'   - calls _ensure_full_commit API to flush change to disk
    val asyncHttpClient = new DefaultHttpAsyncClient();
    val httpParams = asyncHttpClient.getParams()
    httpParams.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
    httpParams.setParameter(CoreConnectionPNames.TCP_NODELAY, true)
    httpParams.setParameter(CoreConnectionPNames.SO_REUSEADDR, true)
    asyncHttpClient.start()
    def request(method: HttpUriRequest): (Int, String) = {
      try {
        val future = asyncHttpClient.execute(method, null: FutureCallback[HttpResponse])
        val response = future.get()
        val statusCode = response.getStatusLine().getStatusCode()
        val content = Source.fromInputStream(response.getEntity().getContent()).getLines.mkString
        return (statusCode, content)
      } catch {
        case e: Exception => return (-1, e.getMessage())
      }
    }
    def flush() = {
      val post = new HttpPost(dbUrl + "/_ensure_full_commit")
      post.setHeader("Content-Type", "application/json")
      request(post)
    }

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

    val sw = new StopWatch()
    mode match {
      case "insert" =>
        val doc = mkdoc(size)
        val docIds = 0.until(count)
        val latch = new CountDownLatch(docIds.length)
        sw.zero()
        docIds.foreach { docId =>
          val put = new HttpPut(dbUrl + "/" + docId + "?batch=ok")
          put.setEntity(new StringEntity(doc))
          asyncHttpClient.execute(put, new FutureCallback[HttpResponse]() {
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
        flush()
        sw.stop()
      case "update-direct" =>
        var doc = mkdoc(size)
        val docIds = 0.until(docCount)
        val latch = new CountDownLatch(docIds.length)
        docIds.foreach { docId =>
          val put = new HttpPut(dbUrl + "/" + docId + "?batch=ok")
          put.setEntity(new StringEntity(doc))
          asyncHttpClient.execute(put, new FutureCallback[HttpResponse]() {
            override def completed(response: HttpResponse) {
              val statusCode = response.getStatusLine().getStatusCode()
              val content = Source.fromInputStream(response.getEntity().getContent()).getLines.mkString
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
        flush()
        println("Created " + docCount + " test documents")
        val objectMapper = new ObjectMapper()
        var docId = 0
        sw.zero()
        0.until(count).foreach { idx =>
          docId = docId + 1
          if (docId >= docCount) {
            docId = 0
          }
          val docUrl = dbUrl + "/" + docId
          response = request(new HttpGet(docUrl))
          doc = response._2
          val jsonObject: ObjectNode = objectMapper.readTree(doc).asInstanceOf[ObjectNode]
          val count = jsonObject.path("count").getIntValue()
          jsonObject.put("count", count + 1)
          val newDoc = objectMapper.writeValueAsString(jsonObject)
          val put = new HttpPut(docUrl)
          put.setEntity(new StringEntity(newDoc))
          asyncHttpClient.execute(put, null: FutureCallback[HttpResponse]).get()
        }
        flush()
        sw.stop()
      case "update-handler" =>
        var doc = mkdoc(size)
        val docIds = 0.until(docCount)
        var latch = new CountDownLatch(docIds.length)
        docIds.foreach { docId =>
          val put = new HttpPut(dbUrl + "/" + docId + "?batch=ok")
          put.setEntity(new StringEntity(doc))
          asyncHttpClient.execute(put, new FutureCallback[HttpResponse]() {
            override def completed(response: HttpResponse) {
              val statusCode = response.getStatusLine().getStatusCode()
              val content = Source.fromInputStream(response.getEntity().getContent()).getLines.mkString
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
        flush()
        println("Created " + docCount + " test documents")
        val objectMapper = new ObjectMapper()
        var docId = 0
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
        latch = new CountDownLatch(count)
        sw.zero()
        0.until(count).foreach { idx =>
          docId = docId + 1
          if (docId >= docCount) {
            docId = 0
          }          
          val put = new HttpPut(dbUrl + "/_design/test_design/_update/increment/" 
              + docId + "?batch=ok")
          asyncHttpClient.execute(put, new FutureCallback[HttpResponse]() {
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
        flush()
        sw.stop()
      case _ => {}
    }
    asyncHttpClient.shutdown()
    val opsPerSec = if (sw.elapsed < 1) 0f else (count.toFloat / sw.elapsed) * 1000f
    println("%d ops in %s, %,.2f ops/sec".format(
      count, sw.elapsedString, opsPerSec))
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
