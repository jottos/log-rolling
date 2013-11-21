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
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

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
  def main(value: Array[String]) = {
    val log = new Logger(this.getClass.getSimpleName)
    log.info("starting logRoller test, baby")

    val hdfsService = new HdfsService()
    val rlist = hdfsService.getAllDirs("/user/logmaster/production")

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
    rlist.map(f=>f.getPath.toString).foreach(f=>
          f match {
        case KeyWithPartition(cluster, key, year, month, day) => {
          val date = Tuple3(year, month, day)
          val newList = if (keyMap.contains(key)) keyMap(key) ++ List(date) else List(date)
          keyMap += key -> newList
          keys += key
        } //println(f"got (cluster,key)=($cluster%s,$key%s, $year%s, $month%s, $day%s)")
        case OtherLogKey(cluster, key) => println(f"got (cluster,key)=($cluster%s,$key%s)")
        case _=> println(f"Error: Noneblown match: $f%s")
    })

    println(f"got keys: $keys%s")
    println(f"got keys: $keyMap%s")
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


