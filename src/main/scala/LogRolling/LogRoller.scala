
/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/23/13
 * Time: 9:33 AM
 */
package com.apixio.service.LogRoller

import scala.collection.mutable.Map
import scala.collection.mutable.MutableList

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
  case class Partition(year: Int, month: Int, day: Int, ordinalDay: Int, location: String, isCached: Boolean) {
    override def toString = f"$year%s,$month%s,$day%s,$ordinalDay%s,$location%s,$isCached%s"
    def sqlPartitionNotation = f"(year=$year, month=$month, day=$day, ordinalday=$ordinalDay,location='$location')"
  }
  // LogKey:  Tuple2[String=>clusterName, String=>metricName]
  type LogKey = Tuple2[String, String]
  type PartitionList = MutableList[Partition]
  type KeyTable = Map[LogKey, PartitionList]
  type Location = String

    // extract the components of a LogKey from the top level location directory
  val LogKeyExtractor = """.*(production|staging)\/([a-zA-Z\d]+).*""".r
  // extract the partition components
  val PartitionExtractor = """.*(production|staging)\/([a-zA-Z\d]+)\/(\d{4})-(\d{2})-(\d{2})""".r

  val log = new Logger(this.getClass.getSimpleName)
  val logOps = new LogDbOperations()
  val hdfs = new HdfsService()
  val keyTable: KeyTable = Map(): KeyTable
  val keyLocation = "/user/logmaster/production/opprouter"

  def main(args: Array[String]) = {
    log.info("this is our new main program")

    // test, add a location, then re-add it, there should only be one when we look
    // both should return true
    log.info(f"adding key location $keyLocation")
    addKey(keyLocation)
    log.info(f"re-adding key location $keyLocation")
    addKey(keyLocation)
    log.info(f"keyTable is: $keyTable%s")

    // hive does not support insert into
    //logOps.putLogKey(("production","opprouter"), partitionList)
    logOps.persistKeyTable(keyTable)

    val newKeyTable = logOps.readKeyTable("/tmp/apxlog_keytable.csv")
    log.info(f"got keytable from file: $newKeyTable%s")
  }

  // operator set
  //
  def hasPartitionsForKey(kt: KeyTable, k: LogKey) : Boolean = kt.contains(k)
  def partitionsForKey(kt: KeyTable, k: LogKey) : PartitionList = if (kt.contains(k)) kt(k) else new PartitionList

  // add partition to keytable and flush new row to hive
  def addPartition(kt: KeyTable, k: LogKey, p: Partition) = println("todo, addPartition")

  /**
   * Given location of log data, go out and determine the partitions needed, add metadata to keyTable and
   * create needed
   * @param location - location of log data as path in S3 and HDFS. It is expected to be the directory where all the
   *                 partitions for this key are located
   *
   * TODO: add search though S3 log data for partitions (only doing HDFS right now)
   */
  def addKey(location: Location): Boolean = {
    val partitionList = new MutableList[Partition]()

    //jos- can we get away with a case class here for the LogKey
    val LogKeyExtractor(cluster, key) = location
    val logKey = (cluster, key)
    log.info(f"addKey: location=$location")
    if (hasPartitionsForKey(keyTable, logKey)) {
      log.info("addKey: location already exists")
      return true
    }
    val partitionDirs = hdfs.getAllDirs(location)

    log.info(s"partition dirs for $location are:\n ${partitionDirs.map(_.getPath.toString).mkString("\n")}")
    try {
      log.info(f"key $logKey%s not found in keytable, creating it")
      partitionDirs.map(fileStatus => fileStatus.getPath.toString) foreach {
        case dirName @ PartitionExtractor(cluster, key, year, month, day) => {
          val ordinalDay = logOps.ordinalDay(year.toInt, month.toInt, day.toInt)
          partitionList += Partition(year.toInt, month.toInt, day.toInt, ordinalDay, dirName, true)
        }
        case dirName @ _ => log.warn(f"Error: No match found for: $dirName%s")
      }
      log.info(s"partitions are $location:\n ${partitionList.map(_ toString).mkString("\n")}")
      keyTable += logKey -> partitionList

      logOps.createPartitionsForKey(logKey: LogKey, partitionList: PartitionList)

      return true
    } catch {
      case ex: Exception => {
        log.error(f"addKey why am i here?: $ex%s")
        return false
      }
    }
  }

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
// jos currently broken
//     val missingPartitions = logOps.checkForAllPartitions(bigKeyMap("production"))

    log.info(f"missing tables $missingProductionTables%s")
//    log.info(f"missing partitions $missingPartitions")
    //val missingTables = checkForAllTables(bigKeyMap("production"), hiveTables("production"))
  }


}

