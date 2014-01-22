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
import com.apixio.utils.HiveConnection
import java.sql.SQLException

import com.apixio.service.LogRolling.LogRoller.{LogKey, PartitionList, KeyTable, Partition}
import java.io.{PrintWriter, File}
import scala.collection.mutable

// helper object, publish some of the useful staticy kinds of things
object LogDbOperations {
  //TODO: migrate the non-instance methods here
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

  val monthBases = List(0,31,59,90,120,151,181,212,243,273,304,334)
  val monthBasesLeapYear = List(0,31,60,91,121,152,182,213,244,274,305,335)
  def isLeapYear(year:Int): Boolean = if (year%100==0) year%400==0 else year%4==0
  /**
   * calculate the ordinal day from y/m/d
   * @param year - year
   * @param month - month of year
   * @param day - day of month
   * @return the ordinal Day
   */
  def ordinalDay(year: Int, month: Int, day: Int) : Int = {
    day + (if (isLeapYear(year))
      monthBasesLeapYear(month-1)
    else
      monthBases(month-1))
  }

  // alternative way to get ordinal day, but way more expensive :)
  val format = "yyyyMMdd"
  val formatter: java.text.SimpleDateFormat = new java.text.SimpleDateFormat(format)
  val calendar = java.util.Calendar.getInstance()
  def ordinalDay_wayMoreExpensive(year: Int, month: Int, day: Int): Int = {
    calendar.setTime(formatter.parse(f"$year$month$day"))
    calendar.get(java.util.Calendar.DAY_OF_YEAR)
  }

}

class LogDbOperations {
  import LogDbOperations.printToFile
  import LogDbOperations.ordinalDay

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
    val backUpKeyTable = f"$keyTableFile.bak"
    keyTableFile renameTo backUpKeyTable

      printToFile(keyTableFile) { pw=>
        keyTable foreach {entry =>
          // kte._1 -> LogKey, kte._2 -> PartitionList
          pw.println(entry._2.map(p=>f"$p").mkString("\n"))
        }
      }
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

    lines foreach { l=>
      val fields = l.split(',').map(_ trim)
      val logKey = LogKey(fields(0), fields(1))
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
      {p=> s"alter table $keyTableName add if not exists partition ${p.sqlPartitionNotationAndLocation}"}
    try {
      log.info("starting sql to create partitions")
      sqlStatements foreach {statement=> hiveConnection.execute(statement)}
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
        val rv = hiveConnection.execute(keyTableCreateScript)
        log.info(f"checkKeyTable returned $rv")
        return rv
      } catch {
        case ex: SQLException => log.error(f"createTable: SQLException: $ex%s")
          return false
        case ex: Exception => log.error(f"createTable: crashed out with $ex%s")
          return false
    }
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
   * hiveTables - get a list of knownTables available in hive
   * @return QueryIterator to table list - drain this into a local structure, you cannot iterate through it more than once
   */
  def hiveTables: List[String] = {
    hiveConnection.fetch("show tables").map(f=>f("tab_name").toString).toList
  }


  private val PartitionTableExtractor = """system=(production|staging)\/source=([a-zA-Z\d]+)\/year=(\d{4})\/month=(\d{2})\/day=(\d{2})\/ordinalday=(\d{2})""".r
  // TODO - this needs to be converted to new Partition case class
  /**
   * getTablePartitions - return a PartitionList for the table provided
   * @param tableName table to scoop up partition info for
   * @return
   */
  def getTablePartitions(tableName: String) : PartitionList = {
    val partitions = new PartitionList()
    try {
      val partitionInfo = hiveConnection.fetch(f"show partitions $tableName%s")
      partitionInfo foreach {
          case PartitionTableExtractor(system, source, year, month, day, ordinalday) =>
            //jos - this has problems, the show partitions DDL does not hold location info
            partitions += Partition(system, source, year.toInt, month.toInt, day.toInt, ordinalday.toInt, "noLocation", isCached = true)

          case f@_=> log.warn(f"getTablePartitions got crap $f%s")
      }
    } catch {
      case ex: Exception =>
        log.error(f"getTablePartitions: failed getting partitions for $tableName")
    }
    partitions
  }

  def checkTableExists(tableName: String) : Boolean = {
    try {
      val tableInfo = hiveConnection.fetch(f"describe formatted $tableName%s")
      // TODO look for col_name == Table Type with value EXTERNAL_TABLE
      true
    } catch {
      case ex: SQLException => return false
      case ex: Exception =>
        log.error(f"checkHiveTable: received unexpected exception $ex%s")
        return false
    }
  }


  /**
   * dropPartition - drop a partition from the master table
   * @param system - cluster that originated log, e.g. production, staging
   * @param source - component that created log e.g. hcc or opprou
   * @param year - the year of log
   * @param month - the month of log
   * @param day - the day of log
   * @param ordinalday - the ordinal day of the log
   * @return
   */
  def dropPartition(system: String, source: String, year: Int, month: Int, day: Int, ordinalday: Int) : Boolean = {
    hiveConnection.execute(f"""alter table $keyTableName drop if exists partition
                                 (system='$system', source='$source', month='$month%s', day='$day', ordinalday='$ordinalday)')""")
    true // returning true, because hive jdbc seems to always return false
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
   * TODO - rework this, use a pull from hdfs to cmpare with keyMap - keyMap will reflect partitions in hive or should, the paranoid function will make sure all three are consitent
   * checkForAllPartitions - given a keymap check that all required partitions specified in map do indeed exist
   * return another keymap representing any missing partitions
   * @param keyMap
   * @return

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

}
