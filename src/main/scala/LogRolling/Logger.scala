/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 9/20/13
 * Time: 8:58 AM
 * To change this template use File | Settings | File Templates.
 */

package com.apixio.service.LogRoller

class Logger(source: String) {
  def error(e : String)
  {
    Console.err.println(f"$source%s: [ERROR] $e%s")
    Console.err.flush()
  }
  def warn(e: String)
  {
    Console.out.println(f"$source%s: [WARN] $e%s")
    Console.out.flush()
  }
  def info(e: String)
  {
    Console.out.println(f"$source%s: [INFO] $e%s")
    Console.out.flush()
  }
}
