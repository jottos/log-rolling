/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/23/13
 * Time: 9:33 AM
 */
package com.apixio.service.LogRolling

import scala.collection.mutable
import org.apache.hadoop.fs.FileStatus
import LogDbOperations.getLines
import LogDbOperations.ordinalDay

/**
 * Central object and control for log rolling
 */
object LogRoller {

  case class LogKey(system: String, source: String)

  /**
   * Partition Metadata
   * @param year - gregorian year
   * @param month - 1-12
   * @param day - 1-31
   * @param ordinalDay - 1-366
   * @param location - path to partition excluding the scheme, host & port
   * @param isCached - is this partition data in hdfs? (changes location)
   */
  case class Partition(system: String, source: String, year: Int, month: Int, day: Int, ordinalDay: Int, location: String, isCached: Boolean) {
    override def toString = f"$system,$source,$year,$month,$day,$ordinalDay,$location,$isCached"
    def sqlPartitionNotationAndLocation = {
      val fqLocation = f"hdfs://$internalHdfsHost:8020$location"
      f"(system='$system', source='$source', year=$year, month=$month, day=$day, ordinalday=$ordinalDay) location '$fqLocation'"
    }
    def sqlPartitionNotation = {
      f"(system='$system', source='$source', year=$year, month=$month, day=$day, ordinalday=$ordinalDay)"
    }
  }
  // LogKey:  Tuple2[String=>system, String=>source]
  //type LogKey = (String, String)
  type PartitionList = mutable.MutableList[Partition]
  type KeyTable = mutable.Map[LogKey, PartitionList]
  type Location = String

  val keyWhiteListFile = "/tmp/apxlog_key_whitelist.csv"

  // internal and external host names for HDFS
  val internalHdfsHost = "ip-10-196-84-183.us-west-1.compute.internal"
  val externalHdfsHost = "54.215.109.178"

  // extract path, system, source, year, month, day
  val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/([\w\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
  // extract path, system, source
  val KeyExtractor = """.*(\/user\/logmaster\/(production|staging)\/([\w\.]+))""".r

  val log = new Logger(this.getClass.getSimpleName)
  val logOps = new LogDbOperations()
  val hdfs = new HdfsService()
  val keyTable: KeyTable = mutable.Map(): KeyTable


  def main(args: Array[String]) = {
    log.info("this is our new main program")

    // 1. get list of source directories from production and staging
    // 2. filter them through a white list
    // 3. pull out the location and add key to db for each
    (hdfs.ls("/user/logmaster/production") ++ hdfs.ls("/user/logmaster/staging"))
      .map(fileStatus=>fileStatus.getPath.toString).filter(inApprovedKeyList(_)) foreach {
      case key@KeyExtractor(keyLocation, system, source) =>
        log.info(f"adding key ($system, $source)")
        addKey(system, source, keyLocation)
      case key@_ => log.warn(f"main loop: have log directory [$key]that does not conform to KeyExtractor")
    }

    logOps.persistKeyTable(keyTable)
  }


  /**
   * Given location of log data, go out and determine the partitions needed, add metadata to keyTable and
   * create needed
   * @param location - location of log data as path in S3 and HDFS. It is expected to be the directory where all the
   *                 partitions for this key are located
   *
   * TODO: add search though S3 log data for partitions (only doing HDFS right now)
   */
  def addKey(system: String, source: String, location: String): Boolean = {
    val partitionList = new PartitionList()
    //TODO !! jos - if we have a central table and not prod and stage tables then log key is only needed to locate files in hdfs (???)
    //TODO jos- can we get away with a case class here for the LogKey
    val logKey = LogKey(system, source)

    if (!hasPartitionsForKey(keyTable, logKey)) {
      val partitionDirs = hdfs.getAllDirs(location)

      try {
        partitionDirs.map(fileStatus=>fileStatus.getPath.toString) foreach {
          case dirName@PartitionExtractor(path, _, _, year, month, day) =>
            val ordDay: Int = ordinalDay(year.toInt, month.toInt, day.toInt)
            partitionList += Partition(system, source, year.toInt, month.toInt, day.toInt, ordDay, path, isCached = true)

          case dirName@_ => log.warn(f"addKey: No match found for: $dirName, ignoring.")
        }
        log.info(s"partitions are $location:\n ${partitionList.map(_ toString).mkString("\n")}")
        keyTable += logKey -> partitionList
        logOps.createPartitionsForKey(partitionList)
      }
      catch {
        case ex: Exception =>
          log.error(f"addKey crapped out creating partitionList: $ex%s")
          false
      }
    }
    else {
      true
    }
  }

  // misc operator set
  //
  def hasPartitionsForKey(kt: KeyTable, k: LogKey) : Boolean = kt.contains(k)
  def partitionsForKey(kt: KeyTable, k: LogKey) : PartitionList = if (kt.contains(k)) kt(k) else new PartitionList

  val keyWhiteList = {
    getLines("/tmp/apxlog_key_whitelist.csv").map(line=>line.split(',') match {case Array(system,source)=>(system, source)}).toSet
    }

  def inApprovedKeyList(path: String): Boolean = {
    val KeyExtractor(_, system, source) = path
    log.info(f"whitelist looking for ($system,$source)")
    keyWhiteList.contains((system, source))
  }

  /**
   * add a single partition to keytable and update hive log table
   * @param logKey - identification of the partitionList we are adding partition to
   * @param partition - metadata for new partition
   * @return
   */
  def addPartition(logKey: LogKey, partition: Partition) = {
    //TODO - add partition to partition list, fail if key does not exist, add partition to logtable
  }

}

