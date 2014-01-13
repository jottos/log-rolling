/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 9/20/13
 * Time: 8:58 AM
 * To change this template use File | Settings | File Templates.
 */
package com.apixio.service.LogRoller

import java.text.SimpleDateFormat
import java.util.Calendar


class Logger(source: String) {
  val cal = Calendar.getInstance()
  val dateFormatter = new SimpleDateFormat("[yyyy-MM-dd'T'HH:mm:ss.S]");

  private def timeStamp = {
    dateFormatter.format(cal.getTime())
  }

  def error(e : String)
  {
    Console.err.println(f"$timeStamp%s $source%s: [ERROR] $e%s")
    Console.err.flush()
  }
  def warn(e: String)
  {
    Console.out.println(f"$timeStamp%s $source%s: [WARN] $e%s")
    Console.out.flush()
  }
  def info(e: String)
  {
    Console.out.println(f"$timeStamp%s $source%s: [INFO] $e%s")
    Console.out.flush()
  }
}
