package LogRolling

import com.apixio.service.LogRoller.HdfsService
import org.scalatest._
/**
 * Created by jos on 1/14/14.
 */
class HdfsServiceTest extends FlatSpec with ShouldMatchers {
  val hdfsService = new HdfsService()

  "hdfsService" should "be able to ls /user/logmaster" in {
    val dirs = hdfsService.ls("/tmp")
    dirs.size should be > 0
  }

  "hdfsService" should "be able to get directory tree for /user/logmaster/production" in {
    val dirTree = hdfsService.getAllDirs("/user/logmaster/production")
  }

  // copy file in
  // copy file out
  // move file inside

}
