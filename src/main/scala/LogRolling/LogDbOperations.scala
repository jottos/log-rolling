/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 1/10/14
 * Time: 5:40 PM
 * To change this template use File | Settings | File Templates.
 */

package com.apixio.service.LogRoller

import scala.collection.mutable.{MutableList, ArrayBuffer, Map}
import scala.io.Source._
import scala.Tuple3
import com.apixio.utils.HiveConnection
import java.sql.SQLException
import com.apixio.service.LogRoller.Logger

// TODO - these imports may need to be switched to LogRollingModel later, currently we are just using an object called logRoller
import logRoller.KeyMap

class LogDbOperations {
  val log = new Logger(this.getClass.getSimpleName)



  /**
   * checkForAllTables - given a list of keys for which we expect that there should be matching knownTables in hive, check
   * the assertion that the knownTables exist in Hive and for those that do not, return a list of hive knownTables (tableNames)
   * that should be created
   * @param keyMap
   * @param knownTables
   * @return
   */
  def checkForAllTables(keyMap: KeyMap, knownTables: Set[String]): ArrayBuffer[String] = {
    val missingTables: ArrayBuffer[String] = ArrayBuffer()
    keyMap.foreach(kvp=>{
      // kvp._1 = tableName, kvp._2 MutableList[Tuple3[Int,Int,Int]]
      if (! knownTables.contains(kvp._1))
        missingTables :+ kvp._1
    })
    missingTables
  }

  /**
   * checkForAllPartitions - given a keymap check that all required partitions specified in map do indeed exist
   * return another keymap representing any missing partitions
   * @param keyMap
   * @return
   */
  def checkForAllPartitions(keyMap: KeyMap) : KeyMap = {
    val neededPartitionsMap: KeyMap = Map()
    keyMap.foreach(kvp=>{
      val neededPartitions: MutableList[PartitionTuple] = MutableList()
      // kvp._1 = tableName, kvp._2 MutableList[Tuple3[Int,Int,Int]]
      val existingPartitions = getTablePartitions(kvp._1)
      kvp._2.foreach(date=>{
        val partition: PartitionTuple = date2partitionTuple(date)
        if (! existingPartitions.contains(partition))
          neededPartitions :+ partition
      })
      neededPartitionsMap(kvp._1) = neededPartitions
    })
    neededPartitionsMap
  }

  type DateTuple = Tuple3[Int, Int, Int]
  type PartitionTuple = Tuple3[Int, Int, Int]
  def date2partitionTuple(date: DateTuple): PartitionTuple = {
    val format = "yyyyMMdd"
    val formatter: java.text.SimpleDateFormat = new java.text.SimpleDateFormat(format)
    val year = date._1
    val month = date._2
    val day = date._3
    val jdate = formatter.parse(f"20$year%s$month%s$day%s")
    val calendar: java.util.Calendar = java.util.Calendar.getInstance()

    calendar.setTime(jdate)
    val week: Int = calendar.get(java.util.Calendar.WEEK_OF_YEAR)
    (date._2, week, date._1)
  }


  //
  // HIVE
  //

  /**
   * hiveConnection - create and maintain access for the application HiveConnection
   */
  private var hiveConn : HiveConnection = null
  def hiveConnection : HiveConnection = {
    if (hiveConn != null)
      hiveConn
    else {
      hiveConn = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")
      hiveConn
    }
  }


  /**
   * createTable - create a partitioned table with the given name
   * @param tableName
   * @return
   */
  def createTable(tableName: String) : Boolean = {
    try {
      hiveConnection.execute(f"create external table if not exists $tableName like logging_master_schema_do_not_remove")
      checkTableExists(tableName)
    } catch {
      case ex: Exception => log.error(f"createTable: crashed out with $ex%s")
        return false
    }
  }

  /**
   * hiveTables - get a list of knownTables available in hive
   * @return QueryIterator to table list - drain this into a local structure, you cannot iterate through it more than once
   */
  def hiveTables: List[String] = {
    hiveConnection.fetch("show tables").map(f=>f("tab_name").toString).toList
  }


  private val PartitionTableExtractor = """month=(\d{2})\/week=(\d{2})\/day=(\d{2})""".r
  type PartitionList = ArrayBuffer[Tuple3[Int,Int,Int]]
  /**
   * getTablePartitions - return a PartitionList for the table provided
   * @param tableName
   * @return
   */
  def getTablePartitions(tableName: String) : PartitionList = {
    val partitions : PartitionList = ArrayBuffer()
    try {
      val partitionInfo = hiveConnection.fetch(f"show partitions $tableName%s")
      partitionInfo.foreach(f=>{
        f("partition") match {
          case PartitionTableExtractor(month, week, day) => {
            val t = Tuple3(month.toInt, week.toInt, day.toInt)
            partitions += Tuple3(month.toInt, week.toInt, day.toInt)
          }
          case _=> log.warn(f"getTablePartitions got crap $f%s")
        }
      })
    } catch {
      case ex: Exception => {
        log.error(f"getTablePartitions: failed getting partitions for $tableName%s")
      }
    }
    partitions
  }

  def checkTableExists(tableName: String) : Boolean = {
    try {
      val tableInfo = hiveConnection.fetch(f"describe formatted $tableName%s")
      // TODO look for col_name == Table Type with value EXTERNAL_TABLE
      return true
    } catch {
      case ex: SQLException => return false
      case ex: Exception => {
        log.error(f"checkHiveTable: received unexpected exception $ex%s")
        return false
      }
    }
  }


  /**
   * createPartition - create a new partition directory for tableName
   * @param tableName
   * @param location
   * @param month
   * @param week
   * @param day
   * @return
   */
  def createPartition(tableName: String, location: String, month: Int, week: Int, day: Int) : Boolean = {
    hiveConnection.execute(f"alter table $tableName%s add if not exists partition (month='$month%s', week='$week%s', day='$day%s') location '$location%s'")
    true // returning true, because hive jdbc seems to always return false
  }

  /**
   * dropPartition - drop a directory partition from table
   * @param tableName
   * @param month
   * @param week
   * @param day
   * @return
   */
  def dropPartition(tableName: String, month: Int, week: Int, day: Int) : Boolean = {
    hiveConnection.execute(f"alter table $tableName%s drop if exists partition (month='$month%s', week='$week%s', day='$day%s')")
    true // returning true, because hive jdbc seems to always return false
  }

}
