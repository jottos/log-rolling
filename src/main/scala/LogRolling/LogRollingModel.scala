
/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/23/13
 * Time: 9:33 AM
 */
package com.apixio.service.LogRoller

import com.apixio.utils.HiveConnection
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.MutableList
import scala.io.Source.fromFile

import java.sql.SQLException

case class PartitionFoo
(val year: Int, val month: Int) {
 override def toString = "this is my 2 string"
}



object logRoller {
  // LogKey:  Tuple2[String=>clusterName, String=>metricName]
  type LogKey = Tuple2[String, String]
  // PartitionMetaData: Tuple6[Int=>yr, Int=>mo, Int=monthDay, Int=>yearDay, String=>location, Boolean=>isCached]
  type PartitionMetaData = Tuple6[Int, Int, Int, Int, String, Boolean]
  type KeyTable = Map[LogKey, MutableList[PartitionMetaData]]

  def year(md: PartitionMetaData) : Int = md._1
  def month(md: PartitionMetaData) : Int = md._2
  def monthDay(md: PartitionMetaData) : Int = md._3
  def yearDay(md: PartitionMetaData) : Int = md._4
  def location(md: PartitionMetaData) : String = md._5
  def isCached(md: PartitionMetaData) : Boolean = md._6

val p = PartitionFoo(2012, 12)
  val x = p.year

  p match{
    case PartitionFoo(2013, m) => println(m)
  }


  type TableMap = Map[String, Set[String]]
  type KeyMap = Map[String, MutableList[Tuple3[Int,Int,Int]]]
  type ClusterKeyMap = Map[String, KeyMap]
  // TODO jos, we need to put this typedef into a package that is shared
  type QueryIterator = Iterator[Map[String,Any]]

  val log = new Logger(this.getClass.getSimpleName)
  val logOps = new LogDbOperations()

  def main(value: Array[String]) = {
    val hdfsService = new HdfsService()

    log.info("Roll Baby Roll..")

    /**
     * TODO
     * operations on the rlist
     * 1) get the list of distinct keys, map to prod vs staging
     * 2) get the list/map of directories (partitions) associated with each key
     * 3) for each key (prod and staging) get the list of partitions that exist today
     * 4) create diff list that needs to generate new knownTables and new partitions
     * pretty much done at this point :)
     */
    val dirList = hdfsService.getAllDirs("/user/logmaster/production")
    val OtherLogKey = """.*(production|staging)\/([a-zA-Z\d]+).*""".r
    val KeyWithPartition = """.*(production|staging)\/([a-zA-Z\d]+)\/(\d{4})-(\d{2})-(\d{2})""".r
    val bigKeyMap : ClusterKeyMap = Map()
    dirList.map(f=>f.getPath.toString).foreach(f=>
      f match {
        case KeyWithPartition(cluster, key, year, month, day) => {
          val date = Tuple3(year.toInt, month.toInt, day.toInt)
          val keyMap : KeyMap =
            if (bigKeyMap.contains(cluster))
              bigKeyMap(cluster)
            else {
              bigKeyMap(cluster) = Map(): KeyMap
              bigKeyMap(cluster)
            }
          keyMap(key) = if (keyMap.contains(key)) keyMap(key) ++= List(date) else MutableList(date)
          log.info(f"adding partition $date%s for key $key%s")
        }
        case OtherLogKey(cluster, key) => log.info(f"got (cluster,key)=($cluster%s,$key%s)")
        case _=> log.warn(f"Error: No match found for: $f%s")
      })
    log.info("")
    log.info("finished dirList.map")
    log.info(f"got keys: $bigKeyMap%s")
    log.info("fetching table map")
    val TableNames = """.*(production|staging)_logs_([a-zA-Z\d]+).*""".r
    var tableMap : TableMap = Map()
    logOps.hiveTables.foreach(f=>
      f match {
        case TableNames(cluster, table) => {
          val newList = if (tableMap.contains(cluster)) tableMap(cluster) ++ Set(table) else Set(table)
          tableMap += cluster -> newList
        }
        case _=> log.warn(f"got hive table that doesn't match: $f%s")
    })
    log.info(f"got knownTables: $tableMap%s")

    log.info("checking for tables")
    val missingProductionTables = logOps.checkForAllTables(bigKeyMap("production"), tableMap("production"))
    val missingPartitions = logOps.checkForAllPartitions(bigKeyMap("production"))

    log.info(f"missing tables $missingProductionTables%s")
    log.info(f"missing partitions $missingPartitions")
    //val missingTables = checkForAllTables(bigKeyMap("production"), hiveTables("production"))
  }

  //
  // MISC
  //
  def getLines(filePath : String) = fromFile(filePath).getLines()

}

