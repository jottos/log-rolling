
/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/23/13
 * Time: 9:33 AM
 */
import com.apixio.utils.HiveConnection
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.collection.mutable.MutableList
import java.sql.SQLException

object runner {
  val log = new Logger(this.getClass.getSimpleName)
  type TableMap = Map[String, Set[String]]
  type KeyMap = Map[String, MutableList[Tuple3[Int,Int,Int]]]
  type ClusterKeyMap = Map[String, KeyMap]
  // TODO jos, we need to put this typedef into a package that is shared
  type QueryIterator = Iterator[Map[String,Any]]

  def main(value: Array[String]) = {

    log.info("Roll Baby Roll..")

    //log.info("running tests...")
    //runTests
    //patternMatchingTest
    //hiveTest
    hdfsTest
    exit(1)
    //log.info("tests done, bye")
    //runTests

    val hdfsService = new HdfsService()
    /**
     * TODO
     * operations on the rlist
     * 1) get the list of distinct keys, map to prod vs staging
     * 2) get the list/map of directories (partitions) associated with each key
     * 3) for each key (prod and staging) get the list of partitions that exist today
     * 4) create diff list that needs to generate new tables and new partitions
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
          //log.info(f"adding partition $year%d, $month%d, $day%d for key $key%s")
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
    hiveTables.foreach(f=>
      f match {
        case TableNames(cluster, table) => {
          val newList = if (tableMap.contains(cluster)) tableMap(cluster) ++ Set(table) else Set(table)
          tableMap += cluster -> newList
        }
        case _=> log.warn(f"got hive table that doesn't match: $f%s")
    })
    log.info(f"got tables: $tableMap%s")
  }

  /**
   * checkForAllTables - given a list of keys for which we expect that there should be matching tables in hive, check
   * the assertion that the tables exist in Hive and for those that do not, return a list of hive tables (tableNames)
   * that should be created
   * @param keyMap
   * @param tables
   * @return
   */
  def checkForAllTables(keyMap: KeyMap, tables: Set[String]): ArrayBuffer[String] = {
    val missingTables: ArrayBuffer[String] = ArrayBuffer()
    keyMap.foreach(kvp=>{
      // kvp._1 = tableName, kvp._2 MutableList[Tuple3[Int,Int,Int]]
      if (! tables.contains(kvp._1))
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
   * hiveTables - get a list of tables available in hive
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

  /**
   * TEST FUNCTIONS
   */
  def runTests = {
    log.info("RUNNING TESTS")
    val cte1 =checkTableExists("production_logs_parserjob_24")
    val cte2 = checkTableExists("production_logs_parserjob_21")
    log.info(f"checkTableExits true:$cte1%b, false:$cte2%b")

    val partitions = getTablePartitions("production_logs_parserjob_epoch")
    log.info("got partion len:" + partitions.length)
    partitions.foreach(println(_))

    if (createTable("jos_logs_test_table"))
      log.info("create and check work properly")
    else
      println("create table :(")

    exit()
  }

  def hiveTest = {
    val log = new Logger(this.getClass.getSimpleName)
    log.info("starting hive jdbc tests")
    val hc = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")

    log.info("simple: show tables")
    val tableList = hc.fetch("show tables")
    tableList.foreach(println(_))

    log.info("complex: select get_json_object(line, '$.status'...")
    val complexQuery = """select  get_json_object(line, '$.status') as parser_status,
                                  get_json_object(line, '$.jobname') as jobname,
                                  count(1) as success_count
                          from staging_logs_persistjob_epoch
                          where week = 40 and get_json_object(line, '$.level') = 'EVENT' group by get_json_object(line, '$.jobname'), get_json_object(line, '$.status')"""
    val complexAnswer = hc.fetch(complexQuery)
    complexAnswer.foreach(println(_))
    log.info("hive Test finsished")
    //
    // from old hive protocol v1 (needs .8 drivers attached to project
    //val hc = new HiveConnectionExperiment("184.169.129.122", 10001)
  }

  def patternMatchingTest = {
    val log = new Logger(this.getClass.getSimpleName)
    log.info("pattern matching test")

    val LogKey = """.*(production|staging)\/([a-zA-Z\d]+).*""".r
    val k1     = """.*(production|staging)\/([a-zA-Z\d]+).*""".r
    val k2     = """.*(production|staging)\/([a-zA-Z\d]+)\/(\d{4})-(\d{2})-(\d{2})""".r
    println("Try two")

    val k1(b1,b2) =
      "hdfs://54.215.109.178:8020/user/logmaster/staging/seqfilecreator/2013-11-12"
    println(f"quick test $b1%s, $b2%s")

    println("Try two")
    val LogKey(c1,c2) = "hdfs://54.215.109.178:8020/user/logmaster/staging/seqfilecreator"
    println(f"quick test $c1%s, $c2%s")

    println("Try three")
    val k2(d1,d2,d3,d4,d5) =
      "hdfs://54.215.109.178:8020/user/logmaster/staging/seqfilecreator/2013-11-12"
    println(f"quick test $d1%s, $d2%s, $d3%s, $d4%s, $d5%s)")
  log.info("pattern matching test finished")
  }

  def hdfsTest = {
    val log = new Logger(this.getClass.getSimpleName)
    log.info("hdfsTest")

    log.info("recursive list of just the dirs")
    val hdfsService = new HdfsService()
    val dirs = hdfsService.getAllDirs("/tmp/apxqueue")
    dirs.foreach(f=>println(f.getPath))

    hdfsService.ls("/user").foreach(f=>println(f.getPath))

    val prodDirs = hdfsService.ls("/user/logmaster/production")
    val stageDirs = hdfsService.ls("/user/logmaster/staging")
    val allDirs = prodDirs ++ stageDirs
    log.info("to lists of dirs")
    allDirs.foreach(f=>println(f.getPath))
    log.info("hdfs test finished")
  }
}

