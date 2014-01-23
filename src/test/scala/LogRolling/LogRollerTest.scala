package com.apixio.service.LogRolling

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers

/**
 * Created by jos on 1/17/14.
 */
class LogRollerTest extends FlatSpec with ShouldMatchers {
  import LogRoller.inApprovedKeyList
  import LogRoller.KeyExtractor
  import LogRoller.LogKey

  // this next bit is jenky but we don't want to test with defaults and we dont' have a
  // better injection solution right now
  val keyTableName = "apx_logmaster_test"
  val keyTableFile =  "/tmp/apxlog_keytable_test.csv"
  val logDbOps = new LogDbOperations(keyTableName, keyTableFile)
  LogRoller.setApxLogmasterName(keyTableName, keyTableFile)

  val keyTable = LogRoller.keyTable
  val keyLocation = "/user/logmaster/production/opprouter"

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to add key from $keyLocation%s" in {
    val KeyExtractor(path, sys, src) = keyLocation
    LogRoller.addKey(sys,src,path) should be (true)
  }

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to re-add key from $keyLocation%s with out any ill effects" in {
    val KeyExtractor(path, sys, src) = keyLocation
    LogRoller.addKey(sys,src,path) should be (true)

//JOS - REMOVE THIS
//    keyTable foreach {entry =>
//      println(entry._2.map(p=>f"$p").mkString("\n"))
//    }
  }

  "persistKeyTable" should "be able to write keytable to disk" in {
    // OUT FOR NOW, we need a tmp table
    // logOps.persistKeyTable(keyTable, keyTableFile)
  }

  "readKeyTable" should "be able to read back a persisted keyTable file and find (\"production\",\"opprouter\")" in {
    val newKeyTable = logDbOps.readKeyTable
    newKeyTable contains LogKey("production", "opprouter") should be (true)
  }

  "inApprovedKeyList" should "fail the path /user/logmaster/production/this.should.fail" in {
    inApprovedKeyList("/user/logmaster/production/this.should.fail") should be (false)
  }
  "inApprovedKeyList" should "fail the path /user/logmaster/production/hcc" in {
    inApprovedKeyList("/user/logmaster/production/hcc") should be (true)
  }
  // TODO - tests left
  // logOps.persistKeyTable(keyTable)

  //logOps.putLogKey(("production","opprouter"), partitionList)
}
