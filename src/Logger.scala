/**
 * Created with IntelliJ IDEA.
 * User: jos
 * Date: 9/20/13
 * Time: 8:58 AM
 * To change this template use File | Settings | File Templates.
 */
class Logger(source: String) {
  def error(e : String)
  {
    Console.err.println(f"$source%s: $e%s")
    Console.err.flush()
  }
  def warn(e: String)
  {
    Console.out.println(f"$source%s: $e%s")
    Console.out.flush()
  }
  def info(e: String)
  {
    Console.out.println(f"$source%s: $e%s")
    Console.out.flush()
  }
}
