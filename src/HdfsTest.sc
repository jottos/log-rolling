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

object HDFSFileService {
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
  implicit def FileStatus2String(f: FileStatus) = f.getPath()

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

  def localLs(path: String) = {
    val files = FileUtil.list(path)
    println(f"got files: $files%s")
    for(f <- files)
      println(f)
  }

  def recursiveDirectoryOnly(path: String) : Array[FileStatus] = {
    val fileStatus = fileSystem.listStatus(path)
    val dirs = fileStatus.filter(_.isDir)
    //dirs.foreach(f=>{val path=f.getPath; println(f"got filtered dir list entry: $path%s")})
    val rdirs = dirs.flatMap(f=>recursiveDirectoryOnly(f.getPath))
    //println(">>>")
    val len = rdirs.length
    //println(f"got rdirs from flatmap: $rdirs%s, len: $len%d")
    //rdirs.foreach(f=>println("dir: "+f.getPath))
    //println("<<<")
    return dirs ++ rdirs
  }

  def ls(path: String) : Array[FileStatus] = {
    val fileStatus = fileSystem.listStatus(path)

    // testing stuff
    println(f"getting files for: $path%s")
    fileStatus.foreach(f=>println(f.getPath))
    println("filtered dir only list")
    fileStatus.filter(_.isDir).foreach(f=>println(f.getPath))

    return fileStatus
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




println("Hadoop FS test")
//HDFSFileService.showConf()
//HDFSFileService.ls("hdfs://54.215.109.178:8020/tmp")
//HDFSFileService.ls("/tmp/conde")
val rlist = HDFSFileService.recursiveDirectoryOnly("/tmp/apxqueue")










rlist.foreach(f=>println(f.getPath))

























































































































































//HDFSFileService.ls("/tmp")

