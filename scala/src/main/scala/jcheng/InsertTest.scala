/**
 * Copyright John Cheng, Aug 12, 2011
 */

/**
 * @author jcheng
 *
 */
package jcheng
import java.util.Arrays.asList
import java.util.concurrent.Future
import java.util.concurrent.CountDownLatch
import scala.io.Source
import org.apache.http.client.methods.HttpDelete
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPut
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.BasicResponseHandler
import org.apache.http.impl.nio.client.DefaultHttpAsyncClient
import org.apache.http.nio.concurrent.FutureCallback
import org.apache.http.params.CoreConnectionPNames
import org.apache.http.HttpResponse
import joptsimple.OptionParser
import joptsimple.OptionSet
import org.apache.http.util.EntityUtils

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
        !List("insert", "update-direct", "update-hander").contains(modeSpec.value(options))) {
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
    val mode = modeSpec.value(options)
    val size = sizeSpec.value(options)
    val doDelete = options.has("D")
    val doBatch = options.has("b")

    // Create an async HttpClient 
    // Define convenience method:
    //   'request' - synchronous GET/PUT/POST, return a status code and the response
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

    val doc = mkdoc(size)
    println("Each document is approximately %s kb".format(doc.length() / 1024))
    val latch = new CountDownLatch(count)
    val sw = new StopWatch().zero()    
    0.until(count).foreach { docId =>
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
    asyncHttpClient.shutdown()
    sw.stop()
    println("%d ops in %s, %,.2f ops/sec".format(
        count, sw.elapsedString, (count.toFloat/sw.elapsed)*1000f))
  }

  var _tmp: StringBuilder = new StringBuilder()
  for (i <- 0 until 1024) {
    _tmp.append("0")
  }
  val str1k: String = _tmp.toString()

  def mkdoc(size: Int): String = {
    val sb = new StringBuilder("{\"prop0\":\"")
    for (i <- 0 until size) {
      sb.append(str1k)
    }
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
