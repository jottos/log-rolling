/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 11/18/13
 * Time: 11:28 AM
 * To change this template use File | Settings | File Templates.
 */
package com.apixio.service.LogRolling

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
    val rFileStatus = if (recurse == true) fileStatus.filter(_.isDir).flatMap(f=>ls(f.getPath)) else Array[FileStatus]()
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


