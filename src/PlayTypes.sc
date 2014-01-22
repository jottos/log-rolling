/**
 * Created by jos on 1/13/14.
 */

import com.sun.corba.se.impl.orbutil.LogKeywords
import scala.collection.mutable.Map
import scala.io.Source._

// Partition: Tuple6[Int=>yr, Int=>mo, Int=monthDay, Int=>yearDay, String=>location, Boolean=>isCached]
case class Partition(year: Int, month: Int, monthDay: Int, yearDay: Int, location: String, isCached: Boolean) {
  override def toString = f"$year%s,$month%s,$monthDay%s,$yearDay%s,$location%s,$isCached%s"
}

object foop {
type LogKey = Tuple2[String, String]
type KeyMap = Map[LogKey, String]

val km : KeyMap = Map()
val k : LogKey = ("a", "b")

km += k -> "first val"
//km += ("a")
}
// tests to see if tuples are interned, we add a LogKey
// ("a","b") and then we use all methods of creating new
// versions of such to pull from the map - so either tuples
// are interned or tuples are equ?
println("printing (a,b)")
println(foop.km(("a","b")))
val k2 :foop.LogKey = ("a", "b")
println(foop.km(k2))
val a = "a"
val b = "b"
println(foop.km((a,b)))
val p = Partition(1,2,3,4,"home", true)
println(f"interpolate an object field, partition.location: ${p.location}%s")


val LocationPathExtractor = """.*(\/user\/logmaster\/(production|staging)\/([a-zA-Z\d]+).*)""".r


val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/([a-zA-Z\d]+)\/(\d{4})-(\d{2})-(\d{2}))""".r



val PartitionExtractor(g1, g2, g3, g4, g5, g6) = "hdfs://54.215.109.178:8020/user/logmaster/production/opprouter/2013-12-24"





println(List(g1, g2, g3, g4, g5, g6))


def getLines(fpath:String) = fromFile(fpath)("UTF-8").getLines()
val li = getLines("/tmp/apxlog_key_whitelist.csv")
println(li)
val ll = li.toList










val lll = ll.map(line=>line.split(',')) collect { case Array(system,source)=>(system, source)}










val s = Set(lll:_*)










val s2 = lll.toSet










val s3 = Set(getLines("/tmp/apxlog_key_whitelist.csv").toList.map(line=>line.split(',')) collect { case Array(system,source)=>(system, source)}:_*)










val s4 = getLines("/tmp/apxlog_key_whitelist.csv").map(line=>line.split(',') match {case Array(system,source)=>(system, source)}).toSet


// create table logdboperation_test_table ( foo int);
import LogDbOperations.hivConnection
hc.execute(f"create table if not exists $testTableName(foofield int)") should be (true)











