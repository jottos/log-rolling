package com.apixio.service.LogRoller

import org.scalatest._
import com.apixio.utils.HiveConnection


/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 1/8/14
 * Time: 6:55 PM
 * To change this template use File | Settings | File Templates.
 */
class LogRollingPrimitivesTest extends FlatSpec with ShouldMatchers {
  val hdfsService = new HdfsService()
  val hc = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")

  "hiveConnection" should "be able to show tables" in {
    val hc = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")
    val tableList = hc.fetch("show tables")
    tableList.size should be > 0
  }

  it should "be able to perform a complex query" in {
    val complexQuery = """select  get_json_object(line, '$.status') as parser_status,
                                  get_json_object(line, '$.jobname') as jobname,
                                  count(1) as success_count
                          from staging_logs_persistjob_epoch
                          where week = 40 and get_json_object(line, '$.level') = 'EVENT' group by get_json_object(line, '$.jobname'), get_json_object(line, '$.status')"""
    val complexAnswer = hc.fetch(complexQuery)

    complexAnswer.size should be > 0
  }

  it should "be able to fail" in { 0 should be (1)}

// jos put these in a hive primitives class and then update this test
/*
  it should "be able to determine if a table exists" in {
    // table exists
    val cte1 = checkTableExists("production_logs_parserjob_24")
    // table does not exist
    val cte2 = checkTableExists("production_logs_parserjob_21")

    cte1 should be (true)
    cte2 should be (false)
  }

  it should "be able to get a list of partitions belonging to a log table" in {
    val partitions = getTablePartitions("production_logs_parserjob_epoch")
    partitions.length should be > 0
  }
*/
  // jos put this in separate class
// class LogRollingPatternMatchTests
  "hdfsService" should "be able to ls /user/logmaster" in {
    val dirs = hdfsService.ls("/tmp")
    dirs.size should be > 0
  }

  "hdfsService" should "be able to get directory tree for /user/logmaster/production" in {
    val dirTree = hdfsService.getAllDirs("/user/logmaster/production")
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
