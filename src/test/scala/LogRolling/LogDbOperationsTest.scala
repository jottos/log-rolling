/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 1/10/14
 * Time: 6:01 PM
 * To change this template use File | Settings | File Templates.
 */
package com.apixio.service.LogRolling

import org.scalatest._
import com.apixio.utils.HiveConnection

class LogDbOperationsTest extends FlatSpec with ShouldMatchers {
  val hc = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")
  val keyTableName = "apx_logmaster_test"
  val keyTableFile =  "/tmp/apxlog_keytable_test.csv"
  val logDbOps = new LogDbOperations(keyTableName, keyTableFile)

  "hiveConnection" should "be able to show tables" in {
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

  val testTableName = "LogDbOperations_test_table"
  "hiveConnection" should f"be able to create temp table  $testTableName" in {
    hc.execute(f"create table if not exists $testTableName(foofield int)") should be (true)
  }

  "hiveConnection" should f"be able to drop a temp table $testTableName" in {
    hc.execute(f"drop table if exists $testTableName") should be (true)
  }

  "hiveConnection" should f"fail if we try to drop a table that doesn't exist - foopy_doopy" in {
    hc.execute(f"drop table foop_doopy") should be (false)
  }

  "hiveConnection" should "be able to drop old keyTable" in {
    hc.execute(f"drop table if exists $keyTableName%s") should be (true)
  }

  // TODO - jos figure out why this fails
  "checkKeyTable" should "be able to create new keytable" in {
    logDbOps.checkKeyTable should be (true)
  }
  // TODO - jos especially figure out why this fails, it's a check, it should not fail
  "checkKeyTable" should "be able to check and find keytable that already exists" in {
    logDbOps.checkKeyTable should be (true)
  }


  "checkTableExists" should "be able to find production_logs_parserjob_24" in {
    val cte =logDbOps.checkTableExists("production_logs_parserjob_24")
    cte should be (true)
  }
  it should "also know that pdocution_logs_parserjob_21 does not exist" in {
    logDbOps.checkTableExists("production_logs_parserjob_21") should be (false)

  }

// TODO: jos - this is going to fail until I create a test version of the apx_logmaster table
  "getTablePartitions" should "be able to get a list of partitions for apx_logmaster" in {
    logDbOps.getTablePartitions("apx_logmaster").length should be > 0
  }
// TODO: jos - this should be where we use checkKeyTable to create test version of apx_logmaster
  "createPartitionTable" should "be able to create the keyTable or verify that the keyTable exists" in {
    logDbOps.checkKeyTable should be (true)
  }
}
