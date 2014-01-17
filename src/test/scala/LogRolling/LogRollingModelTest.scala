package com.apixio.service.LogRoller

import org.scalatest.FlatSpec
import org.scalatest.ShouldMatchers
import com.apixio.service.LogRoller.LogDbOperations

import logRoller.KeyTable

/**
 * Created by jos on 1/17/14.
 */
class LogRollingModelTest extends FlatSpec with ShouldMatchers {
  val logOps = new LogDbOperations()
  val keyTable = logRoller.keyTable
  val keyLocation = logRoller.keyLocation

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to add key from $keyLocation%s" in {
    logRoller.addKey(keyLocation) should be (true)
  }

  // TODO: jos we should have a test key dir somewhere
  "addKey" should f"be able to re-add key from $keyLocation%s with out any ill effects" in {
    logRoller.addKey(keyLocation) should be (true)
  }

  // TODO - tests left
  // logOps.persistKeyTable(keyTable)

  //logOps.putLogKey(("production","opprouter"), partitionList)
}
