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

  val logOps = new LogDbOperations()
  val keyTable = LogRoller.keyTable
  val keyLocation = "/user/logmaster/production/opprouter"
  val keyTableFile = logOps.keyTableFile

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to add key from $keyLocation%s" in {
    val KeyExtractor(path, sys, src) = keyLocation
    LogRoller.addKey(sys,src,path) should be (true)
  }

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to re-add key from $keyLocation%s with out any ill effects" in {
    val KeyExtractor(path, sys, src) = keyLocation
//JOS - REMOVE THIS
    keyTable foreach {entry =>
      println(entry._2.map(p=>f"$p").mkString("\n"))
    }
    LogRoller.addKey(sys,src,path) should be (true)
  }

  "persistKeyTable" should "be able to write keytable to disk" in {
    // OUT FOR NOW, we need a tmp table
    // logOps.persistKeyTable(keyTable, keyTableFile)
  }

  "readKeyTable" should "be able to read back a persisted keyTable file and find (\"production\",\"opprouter\")" in {
    val newKeyTable = logOps.readKeyTable(keyTableFile)
    newKeyTable contains LogKey("production", "opprouter") should be (true)
  }

  "the path /user/logmaster/production/this.should.fail" should "fail inApprovedWhiteList()" in {
    inApprovedKeyList("/user/logmaster/production/this.should.fail") should be ("false")
  }
  "the path /user/logmaster/production/hcc" should "pass inApprovedWhiteList()" in {
    inApprovedKeyList("/user/logmaster/production/hcc") should be ("true")
  }
  // TODO - tests left
  // logOps.persistKeyTable(keyTable)

  //logOps.putLogKey(("production","opprouter"), partitionList)
}
