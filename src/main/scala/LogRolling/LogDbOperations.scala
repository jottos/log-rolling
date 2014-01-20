/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 1/10/14
 * Time: 5:40 PM
 * To change this template use File | Settings | File Templates.
 */

package com.apixio.service.LogRolling

import scala.collection.mutable.{MutableList, ArrayBuffer, Map}
import scala.io.Source._
import scala.Tuple3
import com.apixio.utils.HiveConnection
import java.sql.SQLException

import LogRoller.PartitionList
import LogRoller.KeyTable
import LogRoller.Partition
import java.io.{PrintWriter, File}
import scala.collection.mutable

class LogDbOperations {
  val log = new Logger(this.getClass.getSimpleName)

  val keyTableName = "apx_logmaster"
  val keyTableFile = "/tmp/apxlog_keytable.csv"

  //
  // file handling support functions
  //
  /**
   * convert string to File - need to be careful
   * e.g.
   *   "file1" renameTo "file2
   *   "file1" delete
   *
   * @param s - string path of file
   * @return File for path s
   */
  implicit def file(s: String) = new File(s)

  /**
   * create a printwriter from
   * @param file - where to print to
   * @param printingFunction  - closure passed in by caller that will print to print writer
   * @return Unit
   */
  def printToFile(file: File)(printingFunction: PrintWriter => Unit) {
    val p = new PrintWriter(file, "UTF-8")
    try {
      printingFunction(p)
    } finally {
      p.close()
    }
  }

  /**
   * Scoop up all the lines from a file and present as an iterable
   * @param filePath - string path of file to read
   * @return - iterator of lines from file
   */
  def getLines(filePath : String) = fromFile(filePath)("UTF-8").getLines()

  /**
   * write keytable to disk, move current keyTable file to tmp location first if it exists, then write out new file
   * delete old keyTable file if new table is successfully written to disk
   *
   * Fields 0 & 1 are LogKey
   * Fields 2 - 7 are the partition metadata
   *
   * @param keyTable - keyTable to persist
   * @param keyTableFile - file to persist keyTable to
   */
  def persistKeyTable(keyTable: KeyTable, keyTableFile: String = keyTableFile) = {
    keyTable.foreach(kte=>{
      // kte._1 -> LogKey, kte._2 -> PartitionList
      val backUpKeyTable = f"$keyTableFile%s.bak"

      keyTableFile renameTo backUpKeyTable

      printToFile(keyTableFile) { pw=>
        //pw.println(kte._2.map(p=>s"$cluster,$source,${p}").mkString("\n"))
        pw.println(kte._2.map(p=>f"$p").mkString("\n"))
      }
    })
  }

  /**
   * Read and return a KeyTable contained in keyTable csv file
   * Fields 0 & 1 are LogKey
   * Fields 2 - 7 are the partition metadata
   *
   * @param keyTableFile  path to file that contains keyTable
   * @return a KeyTable contained in file
   */
  def readKeyTable(keyTableFile: String = keyTableFile) : KeyTable = {
    val lines = fromFile(keyTableFile: String).getLines()
    val keyTable : KeyTable = mutable.Map()

    lines.foreach { l=>
      val fields = l.split(',').map(_ trim)
      val logKey = (fields(0), fields(1))
      val partition = Partition(fields(0), fields(1), fields(2).toInt, fields(3).toInt, fields(4).toInt, fields(5).toInt, fields(6), fields(7).toBoolean)
      if (! keyTable.contains(logKey)) {
        keyTable += logKey -> new PartitionList()
      }
      keyTable(logKey) += partition
    }
    keyTable
  }

/*
  f"alter table $tableName%s add if not exists partition (year='$year%s', month='$month%s', monthday='$monthday', yearday='$yearday') location '$location%s';"

  f"create external table if not exists $tableName%s like logging_master_schema_do_not_remove"

  f"alter table $tableName%s drop if exists partition (year='$year%s', month='$month%s', monthday='$monthday', yearday='$yearday');"
*/

  /**
   *
   * @param partitions - list of partitions that need to be created in the logging table, keyTable is currently a modual
   *                   variable because we are operating under a model of having a single logmaster table
   * @return
   */
  def createPartitionsForKey(partitions: PartitionList) : Boolean = {
    val sqlStatements = partitions map
      {p=> s"alter table $keyTableName add if not exists partition ${p.sqlPartitionNotation}"}
    try {
      log.info("starting sql to create partitions")
      sqlStatements foreach {statement=> hiveConnection.execute(statement)}
      //      return
      log.info("finished sql to create partitions")
      true // remove me after removing logs
    } catch {
      case ex: Exception => log.error(f"createPartitions: failed on inserts with $ex%s")
        return false
    }
  }



  /**
   * create the logmaster keyTable if it does not exist
   * @return
   */
   def checkKeyTable : Boolean = {
     val keyTableCreateScript =
       s"""CREATE EXTERNAL TABLE IF NOT EXISTS $keyTableName (
          line STRING COMMENT 'log line, formated in json'
          ) COMMENT 'universal apixio log table, contains partitions for all systems and logging keys'
          PARTITIONED BY (
          system     STRING COMMENT 'the system the source came from, e.g. production or staging',
          source     STRING COMMENT 'the system component that generated the log',
          year       INT COMMENT 'the year in which the log was created',
          month      INT COMMENT 'the month of the year the log was created',
          day        INT COMMENT 'the day of the month the log was created',
          ordinalday INT COMMENT 'the day of year the log was created'
          )
         ROW FORMAT DELIMITED LINES TERMINATED BY '\n'
         STORED AS TEXTFILE"""

      try {
        hiveConnection.execute(keyTableCreateScript)
      } catch {
        case ex: Exception => log.error(f"createTable: crashed out with $ex%s")
          false
    }
  }

// JOS - deprecated, this and checkForAllTables is just here until we clean up
type KeyMap = mutable.Map[String,mutable.MutableList[(Int,Int,Int)]]
  /**
   * checkForAllTables - given a list of keys for which we expect that there should be matching knownTables in hive, check
   * the assertion that the knownTables exist in Hive and for those that do not, return a list of hive knownTables (tableNames)
   * that should be created
   * @param keyMap - in memory db of all partitions
   * @param knownTables tables in the db we know of (DEPRECATED)
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
   * DEPRECATED - THIS METHOD NO LONGER RELEVANT, may become so again if we move to MySQL or ApacheDrill for metadata
   * Given a logKey and a list of partitions, persist this as a set of
   * rows in the Hive backing store for the logging model
   *
   * @ param key
   * @ param partitions
   * /

  def putLogKey(key: LogKey, partitions: PartitionList) = {
    // build query
    // TODO: jos, try doing a partition helper class for StringContext

    var inserts: String = ""

    // first check to make sure key is not in table, if it is
    // ? bail or ?overwrite
    partitions.foreach(p=>{
      val year = p.year
      val month = p.month
      val monthday = p.monthDay
      val yearday = p.yearDay
      val location = p.location
      val iscached = p.isCached

      inserts += f"insert into $keyTableName%s values($year%s, $month%s, $monthday%s, $yearday%s, '$location%s', $iscached%s)\n"
    })
    try {
      hiveConnection.execute(inserts)
    } catch {
      case ex: Exception => log.error(f"putLogKey: failed on inserts with $ex%s")
    }
    println(inserts)

  }
  */

  /**
   * checkForAllPartitions - given a keymap check that all required partitions specified in map do indeed exist
   * return another keymap representing any missing partitions
   * @param keyMap
   * @return
   */
  /*
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
*/
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
  def createPartitionTable(tableName: String) : Boolean = {
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

  // TODO - this needs to be converted to new Partition case class
  /**
   * getTablePartitions - return a PartitionList for the table provided
   * @param tableName
   * @return
   */
  def getTablePartitions(tableName: String) : MutableList[Tuple3[Int,Int,Int]] = {
    val partitions : MutableList[Tuple3[Int,Int,Int]] = MutableList()
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

  val monthBases = List(0,31,59,90,120,151,181,212,243,273,304,334)
  val monthBasesLeapYear = List(0,31,60,91,121,152,182,213,244,274,305,335)
  def isLeapYear(year:Int): Boolean = if (year%100==0) year%400==0 else year%4==0

  /**
   * calculate the ordinal day from y/m/d
   * @param year
   * @param month
   * @param day
   * @return the ordinal Day
   */
  def ordinalDay(year: Int, month: Int, day: Int) : Int = {
    return day + (if (isLeapYear(year))
      monthBasesLeapYear(month-1)
    else
      monthBases(month-1))
  }
}
