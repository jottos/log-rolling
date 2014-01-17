package com.apixio.service.LogRoller

import org.scalatest._
import com.apixio.utils.HiveConnection
import scala.collection.mutable.{MutableList, Map}


/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 1/8/14
 * Time: 6:55 PM
 * To change this template use File | Settings | File Templates.
 */
class LogRollingPrimitivesTest extends FlatSpec with ShouldMatchers {
  case class Partition(year: Int, month: Int, monthDay: Int, yearDay: Int, location: String, isCached: Boolean) {
    override def toString = f"$year%s,$month%s,$monthDay%s,$yearDay%s,$location%s,$isCached%s"
  }
  //TODO: tests that show that LogKeys are values and no matter how many times we construct them or from where they always are eqv? and will pull the right item from our Map()
  type LogKey = Tuple2[String, String]
  type KeyTable = Map[LogKey, MutableList[Partition]]
  // example partition instance
  val p = Partition(2012, 12, 1, 334, "/user/logmaster/production/fooKey", false)
  val yr = p.year

  val kt : KeyTable = Map()
  kt += ("a", "b") -> MutableList(p)


  p match{
    case Partition(2012, m, md, yd, loc, cached) => println(m)
  }


  "the LogKey pattern" should "be able to match Cluster and Key" in {
    val LogKey = """.*(production|staging)\/([a-zA-Z\d]+).*""".r
    val LogKey(cluster,key) = "hdfs://54.215.109.178:8020/user/logmaster/staging/seqfilecreator"
    cluster should be ("staging")
    key should be ("seqfilecreator")
  }

  "the TableName pattern" should "be able to match Cluster and tableName" in {
    val TableNames = """.*(production|staging)_logs_([a-zA-Z\d]+).*""".r
    val TableNames(cluster1, table1) = "production_logs_loggingKey_24"
    cluster1 should be ("production")
    table1 should be ("loggingKey")

    val TableNames(cluster2, table2) = "staging_logs_foopKey_epoch"
    cluster2 should be ("staging")
    table2 should be ("foopKey")
  }

  "the KeyWithPartition pattern" should "be able to match Cluster, Key and Partition Elements" in {
    val KeyWithPartition = """.*(production|staging)\/([a-zA-Z\d]+)\/(\d{4})-(\d{2})-(\d{2})""".r
    val KeyWithPartition(cluster,key, year, month, day) = "/user/logmaster/production/hcc/2014-01-10"

    cluster should be ("production")
    key should be ("hcc")
    year should be ("2014")
    month should be ("01")
    day should be ("10")
  }
}
