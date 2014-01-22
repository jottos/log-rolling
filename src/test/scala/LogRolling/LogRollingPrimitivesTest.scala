package com.apixio.service.LogRolling

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
  import LogRoller.Partition
  import LogRoller.KeyExtractor
  import LogRoller.LogKey
  import LogRoller.KeyTable


  "a partition object we created" should "be able to match based on fields" in {
    val p = Partition("production", "aSourceKey", 2012, 12, 1, 334, "/user/logmaster/production/fooKey", false)

    p match{
      case Partition(system, source, 2012, m, d, od, loc, cached) => println(m)
        system should be ("production")
        source should be ("aSourceKey")
        m should be (12)
        d should be (1)
        od should be (334)
        loc should be("/user/logmaster/production/fooKey")
        cached should be (false)
    }
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

  "KeyExtractor" should "be able to match path, system and source from /user/logmaster/production/sourceKey" in {
    val KeyExtractor(path, system, source) = "/user/logmaster/production/sourceKey"
    path should be ("/user/logmaster/production/sourceKey")
    system should be ("production")
    source should be ("sourceKey")
  }

  "PartitionFromPartitionTableExtractor" should "be able to match system,source,year,month,day,ordinalday" in {
    //val PartitionTableExtractor = """system=(production|staging)\/source=([a-zA-Z\d]+)\/year=(\d{4})\/month=(\d+)\/day=(\d{2})\/ordinalday=(\d{2})""".r
    val PartitionTableExtractor = """system=(production|staging)\/source=([a-zA-Z\d]+)\/year=(\d{4})\/month=(\d{1,2})\/day=(\d{1,2})\/ordinalday=(\d{1,2})""".r
    val PartitionTableExtractor(system,source,year,month,day,ordinalday) = "system=production/source=opprouter/year=2014/month=1/day=20/ordinalday=20"

    system should be ("production")
    source should be ("opprouter")
    year should be ("2014")
    month should be ("1")
    day should be ("20")
    ordinalday should be ("20")
  }

  "the PartitionFromHdfsPathExtractor pattern" should "be able to match Path, Cluster, Metric, Year, Month & Day groups" in {
    val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/([a-zA-Z_\d\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
    val PartitionExtractor(path, cluster, source, year, month, day) = "hdfs://54.215.109.178:8020/user/logmaster/production/PatientManifest/2013-12-24"

    path should be ("/user/logmaster/production/PatientManifest/2013-12-24")
    cluster should be ("production")
    source should be ("PatientManifest")
    year should be ("2013")
    month should be ("12")
    day should be ("24")
  }

  it should "also be able to handle funky names for sources like Patient_manifest.foopy" in {
    val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/([a-zA-Z_\d\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
    val PartitionExtractor(_, _, source, _, _, _) = "hdfs://54.215.109.178:8020/user/logmaster/production/Patient_manifest.foopy/2013-12-24"

    source should be ("Patient_manifest.foopy")
  }

  "the NEW PartitionFromHdfsPathExtractor pattern" should "be able to match Path, Cluster, Metric, Year, Month & Day groups" in {
    //val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/(\w\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
    //l PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/([a-zA-Z_\d\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
    val PartitionExtractor = """.*(\/user\/logmaster\/(production|staging)\/([\w\.]+)\/(\d{4})-(\d{2})-(\d{2}))""".r
    val PartitionExtractor(path, cluster, source, year, month, day) = "hdfs://54.215.109.178:8020/user/logmaster/production/PatientManifest.foop_doop/2013-12-24"

    path should be ("/user/logmaster/production/PatientManifest.foop_doop/2013-12-24")
    cluster should be ("production")
    source should be ("PatientManifest.foop_doop")
    year should be ("2013")
    month should be ("12")
    day should be ("24")
  }

  }
