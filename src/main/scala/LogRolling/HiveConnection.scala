package com.apixio.utils

import java.sql.DriverManager
import scala.collection.immutable.TreeMap
import com.apixio.service.LogRolling.Logger

/**
 * A simple wrapper to access sql databases or hive and get to the
 * jdbc layer without too much problems.
 * Created with IntelliJ IDEA.
 * User: vvyas
 * User: jos
 * Date: 10/9/13
 * Time: 10:09 AM
 */

class HiveConnection(jdbcUrl:String, username:String, password:String, driverName:String = "org.apache.hive.jdbc.HiveDriver") {
  val log = new Logger(this.getClass.getSimpleName)
  Class.forName(driverName)
  type QueryIterator = Iterator[Map[String,Any]]
  private val con = DriverManager.getConnection(jdbcUrl,username,password)

  // TODO - really could use a batch

  def execute(query:String) : Boolean = {
//    log.info(f"got query $query")
    val stmt = con.createStatement()
    stmt.execute(query)
  }

  def fetch(query:String) = {
//    log.info(f"got query $query")
    val stmt = con.createStatement()
    val results = stmt.executeQuery(query)

    // construct an inline iterator that lets us run through a result set and once
    // we are all done, releases any used resources like connections and statements
    //new Iterator[Map[String,AnyRef]] {
    new QueryIterator {

      def hasNext: Boolean = {
        if(!results.next()) {
          stmt.close()
          false
        } else true
      }

      def next() = {
          (1 to results.getMetaData.getColumnCount)
            .map((i)=>results.getMetaData.getColumnName(i)->results.getObject(i))
            .foldLeft(new TreeMap[String,AnyRef]())(_ + _)
      }
    }
  }
}
