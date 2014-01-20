package com.apixio.service.LogRolling

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers

/**
 * Created by jos on 1/17/14.
 */
class LogRollerTest extends FlatSpec with ShouldMatchers {
  val logOps = new LogDbOperations()
  val keyTable = LogRoller.keyTable
  val keyLocation = "/user/logmaster/production/opprouter"
  val keyTableFile = logOps.keyTableFile

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to add key from $keyLocation%s" in {
    LogRoller.addKey(keyLocation) should be (true)
  }

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to re-add key from $keyLocation%s with out any ill effects" in {
    LogRoller.addKey(keyLocation) should be (true)
  }

  "persistKeyTable" should "be able to write keytable to disk" in {
    logOps.persistKeyTable(keyTable, keyTableFile)
  }

  "readKeyTable" should "be able to read back a persisted keyTable file and find (\"production\",\"opprouter\")" in {
    val newKeyTable = logOps.readKeyTable(keyTableFile)
    newKeyTable contains ("production", "opprouter") should be (true)
  }
  // TODO - tests left
  // logOps.persistKeyTable(keyTable)

  //logOps.putLogKey(("production","opprouter"), partitionList)
}
