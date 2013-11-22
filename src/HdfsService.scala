/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/18/13
 * Time: 11:28 AM
 * To change this template use File | Settings | File Templates.
 */
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.sql.SQLException
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import scala.collection.mutable.ArrayBuffer

class HdfsService {
  private val conf = new Configuration()
  private val hdfsCoreSitePath = new Path("/User/jos/Documents/workspace/hadoop/hadoop-1.2.2/conf/core-site.xml")
  private val hdfsHDFSSitePath = new Path("/User/jos/Documents/workspace/hadoop/hadoop-1.2.2/conf/hdfs-site.xml")

  conf.addResource(hdfsCoreSitePath)
  conf.addResource(hdfsHDFSSitePath)
  conf.set("fs.default.name","hdfs://54.215.109.178:8020")
  conf.set("hadoop.job.ugi", "hdfs");

  private val fileSystem = FileSystem.get(conf)

  implicit def String2Path(path: String) = new Path(path)
  implicit def Path2String(path: Path) = path.toString
  implicit def String2File(path: String) = new File(path)


  def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    var b = new Array[Byte](1024)
    var numBytes = in.read(b)
    while (numBytes > 0) {
      out.write(b, 0, numBytes)
      numBytes = in.read(b)
    }
    in.close()
    out.close()
  }


  def showConf() = {
    //for (c <- conf)
      //println(c)
  }

  /**
   * localLs returns an array of strings to file(s) at the path provided
   *
   * @param path - the path to a file or directory
   * @return - array of strings describing file(s) at path
   */
  def localLs(path: String) : Array[String] = {
    val files = FileUtil.list(path)
    files
  }

  /**
   * return a list of all of the directories that exist under the path provided
   * @param path - a HDFS file system location to start listing
   * @return - an Array of Hadoop FileStatus objects representing all of the directories
   *         that can recursively be found under the provided path
   */
  def getAllDirs(path: String) : Array[FileStatus] = {
    val dirs = fileSystem.listStatus(path).filter(_.isDir)
    val rdirs = dirs.flatMap(d=>getAllDirs(d.getPath))
    return dirs ++ rdirs
  }

  /**
   *
   * @param path - an HDFS filesystem location to start directory listing
   * @param recurse - descend through all directories
   * @return - an Array of Hadoop FileStatus objects
   */
  def ls(path: String, recurse: Boolean = false) : Array[FileStatus] = {
    val fileStatus = fileSystem.listStatus(path)
    val rFileStatus = if (recurse == true) fileStatus.filter(_.isDir).flatMap(f=>ls(f.getPath)) else null
    return fileStatus ++ rFileStatus
  }

  def removeFile(filename: String): Boolean = {
    fileSystem.delete(filename, true)
  }

  def getFile(filename: String): InputStream = {
    fileSystem.open(filename)
  }
  def createFolder(folderPath: String): Unit = {
    if (!fileSystem.exists(folderPath)) {
      fileSystem.mkdirs(folderPath)
    }
  }
}



object runner {
  val log = new Logger(this.getClass.getSimpleName)
  def main(value: Array[String]) = {

    log.info("starting logRoller test, baby")

    val cte1 =checkTableExists("production_logs_parserjob_24")
    val cte2 = checkTableExists("production_logs_parserjob_21")
    println(f"checkTableExits true:$cte1%b, false:$cte2%b")

    val partitions = getTablePartitions("production_logs_parserjob_epoch")
    println("got partion len:" + partitions.length)
    partitions.foreach(println(_))

    if (createTable("jos_logs_test_table"))
        println("create and check work")
    else
      println("create table :(")

    exit()

    val hdfsService = new HdfsService()
    val dlist = hdfsService.getAllDirs("/user/logmaster/production")
    val tables = hiveTables
    /**
     * TODO
     * operations on the rlist
     * 1) get the list of distinct keys, map to prod vs staging
     * 2) get the list/map of directories (partitions) associated with each key
     * 3) for each key (prod and staging) get the list of partitions that exist today
     * 4) create diff list that needs to generate new tables and new partitions
     * pretty much done at this point :)
     */
    val OtherLogKey = """.*(production|staging)\/([a-zA-Z\d]+).*""".r
    val KeyWithPartition = """.*(production|staging)\/([a-zA-Z\d]+)\/(\d{4})-(\d{2})-(\d{2})""".r
    var keys : Set[String] = Set()
    var keyMap : Map[String, List[(_,_,_)]] = Map()
    dlist.map(f=>f.getPath.toString).foreach(f=>
      f match {
        case KeyWithPartition(cluster, key, year, month, day) => {
          val date = Tuple3(year, month, day)
          val newList = if (keyMap.contains(key)) keyMap(key) ++ List(date) else List(date)
          keyMap += key -> newList
          keys += key  // for debug
        }
        case OtherLogKey(cluster, key) => println(f"got (cluster,key)=($cluster%s,$key%s)")
        case _=> log.warn(f"Error: No match found for: $f%s")
    })

    log.info(f"got keys: $keys%s")
    log.info(f"got keys: $keyMap%s")

    val TableNames = """.*(production|staging)_logs_([a-zA-Z\d]+).*""".r
    var tableMap : Map[String, Set[String]] = Map()
    hiveTables.map(f=>f("tab_name")).foreach(f=>
      f match {
        case TableNames(cluster, table) => {
          val newList = if (tableMap.contains(cluster)) tableMap(cluster) ++ Set(table) else Set(table)
          tableMap += cluster -> newList
        }
        case _=> log.warn(f"got hive table that doesn't match: $f%s")
    })
    println(f"got tables: $tableMap%s")
  }

  // TODO jos, we need to put this typedef into a package that is shared
  type QueryIterator = Iterator[Map[String,Any]]

  var hiveConn : HiveConnection = null
  def hiveConnection : HiveConnection = {
    if (hiveConn != null)
      hiveConn
    else {
      hiveConn = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")
      hiveConn
    }
  }


  def createTable(tableName: String) : Boolean = {
    try {
      hiveConnection.execute(f"create external table if not exists $tableName like logging_master_schema_do_not_remove")
      checkTableExists(tableName)
    } catch {
      case ex: Exception => log.error(f"createTable: crashed out with $ex%s")
      return false
    }
  }


  def hiveTables : QueryIterator = {
    hiveConnection.fetch("show tables")
  }

  val PartitionTableExtractor = """month=(\d{2})\/week=(\d{2})\/day=(\d{2})""".r
  type PartitionList = ArrayBuffer[Tuple3[Int,Int,Int]]
  def getTablePartitions(tableName: String) : PartitionList = {
    var partitions : PartitionList = ArrayBuffer()
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

  def hiveTest = {
    println("starting hive jdbc tests")
    val hc = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")
    val tableList = hc.fetch("show tables")
    tableList.foreach(println(_))

    val complexQuery = """select  get_json_object(line, '$.status') as parser_status,
                                  get_json_object(line, '$.jobname') as jobname,
                                  count(1) as success_count
                          from staging_logs_persistjob_epoch
                          where week = 40 and get_json_object(line, '$.level') = 'EVENT' group by get_json_object(line, '$.jobname'), get_json_object(line, '$.status')"""
    val complexAnswer = hc.fetch(complexQuery)
    complexAnswer.foreach(println(_))

    val log = new Logger(this.getClass.getSimpleName)

    //
    // from old hive protocol v1 (needs .8 drivers attached to project
    //val hc = new HiveConnectionExperiment("184.169.129.122", 10001)
  }

  def patterMatchingTest = {
    println("Hadoop FS test")

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
  }
}

