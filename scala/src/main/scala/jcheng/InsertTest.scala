/**
 * Copyright John Cheng, Aug 12, 2011
 */

/**
 * @author jcheng
 *
 */
package jcheng

object InsertTest extends optional.Application {

  def main(server: Option[String], update: Boolean): Unit = {
    val _server = server getOrElse "http://localhost:5984"
    println(update)
  }

}
