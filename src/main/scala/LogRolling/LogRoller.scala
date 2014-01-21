/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/23/13
 * Time: 9:33 AM
 */
package com.apixio.service.LogRolling

import scala.collection.mutable
import org.apache.hadoop.fs.FileStatus

/**
 * Central object and control for log rolling
 */
object LogRoller {

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
  type LogKey = (String, String)
  type PartitionList = mutable.MutableList[Partition]
  type KeyTable = mutable.Map[LogKey, PartitionList]
  type Location = String

  // internal and external host names for HDFS
  val internalHdfsHost = "ip-10-196-84-183.us-west-1.compute.internal"
  val externalHdfsHost = "54.215.109.178"

  // extract path, system, source, year, month, day
  val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/(\w\d\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
  // extract key (system,source) + path to this key
  val KeyExtractor = """.*(\/user\/logmaster\/(production|staging)\/([\w\d\.]+))""".r

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
        //log.info(f"adding key $keyLocation")
        println(f"""\"$system\"->\"$source\""")
        //addKey(keyLocation)
      case key@_ => log.warn(f"main loop: have log directory [$key]that does not conform to KeyExtractor")
    }

    logOps.persistKeyTable(keyTable)
  }

  // operator set
  //
  def hasPartitionsForKey(kt: KeyTable, k: LogKey) : Boolean = kt.contains(k)
  def partitionsForKey(kt: KeyTable, k: LogKey) : PartitionList = if (kt.contains(k)) kt(k) else new PartitionList

  val keyWhitelist = Set("production"->"hcc")
  def inApprovedKeyList(path: String): Boolean = {
    val KeyExtractor(_, system, source) = path
    keyWhitelist.contains((system, source))
    true
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


  /**
   * Given location of log data, go out and determine the partitions needed, add metadata to keyTable and
   * create needed
   * @param location - location of log data as path in S3 and HDFS. It is expected to be the directory where all the
   *                 partitions for this key are located
   *
   * TODO: add search though S3 log data for partitions (only doing HDFS right now)
   */
  def addKey(location: Location): Boolean = {
    val partitionList = new mutable.MutableList[Partition]()
    //TODO !! jos - if we have a central table and not prod and stage tables then log key is only needed to locate files in hdfs (???)
    //TODO jos- can we get away with a case class here for the LogKey
    val KeyExtractor(path, system, source) = location
    val logKey = (system, source)

    if (!hasPartitionsForKey(keyTable, logKey)) {
      val partitionDirs = hdfs.getAllDirs(location)

      try {
        partitionDirs.map(fileStatus => fileStatus.getPath.toString) foreach {
          case dirName@PartitionExtractor(_, _, _, year, month, day) =>
            val ordinalDay: Int = logOps.ordinalDay(year.toInt, month.toInt, day.toInt)
            partitionList += Partition(system, source, year.toInt, month.toInt, day.toInt, ordinalDay, path, isCached = true)

          case dirName@_ => log.warn(f"Error: No match found for: $dirName")
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

/*
  //
  // TODO: JOS- move most of this to tests, through out the rest

  type KeyMap = Map[String, MutableList[Tuple3[Int,Int,Int]]]

  def oldMain(value: Array[String]) = {
    log.info("Roll Baby Roll..")
    type TableMap = Map[String, Set[String]]

    type ClusterKeyMap = Map[String, KeyMap]
    // TODO jos, we need to put this typedef into a package that is shared

    /**
     * TODO
     * operations on the rlist
     * 1) get the list of distinct keys, map to prod vs staging
     * 2) get the list/map of directories (partitions) associated with each key
     * 3) for each key (prod and staging) get the list of partitions that exist today
     * 4) create diff list that needs to generate new knownTables and new partitions
     * pretty much done at this point :)
     */
    val dirList = hdfs.getAllDirs("/user/logmaster/production")

    val bigKeyMap : ClusterKeyMap = Map()
    dirList.map(f=>f.getPath.toString).foreach(f=>
      f match {
        case PartitionExtractor(cluster, key, year, month, day) => {
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
        case LogKeyExtractor(cluster, key) => log.info(f"got (cluster,key)=($cluster%s,$key%s)")
        case _=> log.warn(f"Error: No match found for: $f%s")
      })
    log.info("")
    log.info("finished dirList.map")
    log.info(f"got keys: $bigKeyMap%s")
    log.info("fetching table map")
    val TableNames = """.*(production|staging)_logs_([a-zA-Z\d]+).*""".r
    var tableMap : TableMap = Map()
    logOps.hiveTables.foreach(f =>
      f match {
        case TableNames(cluster, table) =>
          val newList = if (tableMap.contains(cluster)) tableMap(cluster) ++ Set(table) else Set(table)
          tableMap += cluster -> newList

        case _ => log.warn(f"got hive table that doesn't match: $f%s")
      })
    log.info(f"got knownTables: $tableMap%s")

    log.info("checking for tables")
    val missingProductionTables = logOps.checkForAllTables(bigKeyMap("production"), tableMap("production"))
// jos currently broken
//     val missingPartitions = logOps.checkForAllPartitions(bigKeyMap("production"))

    log.info(f"missing tables $missingProductionTables%s")
//    log.info(f"missing partitions $missingPartitions")
    //val missingTables = checkForAllTables(bigKeyMap("production"), hiveTables("production"))
  }
*/

}

