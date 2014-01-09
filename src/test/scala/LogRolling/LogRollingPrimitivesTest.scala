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

  "hiveConnection" should "be able to show tables" in {
    val hc = new HiveConnection("jdbc:hive2://184.169.209.24:10000/default", "hive", "")
    val tableList = hc.fetch("show tables")

    tableList.size should be > 0
  }

  it should "be able to fail" in { 0 should be (1)}

  "hdfsService" should "be able to ls /tmp" in {
    val hdfsService = new HdfsService()
    val dirs = hdfsService.ls("/tmp")

    dirs.size should be > 0
  }

}
