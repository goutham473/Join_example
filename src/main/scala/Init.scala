import conf.AppConf
import model.Process
import util.SparkSessionProvider

import scala.util.{Failure, Success, Try}

object Init extends App with SparkSessionProvider{

  val appConf = AppConf(args(0), args(1), args(2), args(3))
  Try(Process(appConf, sparkSession)) match {
    case Success(_) => sparkSession.stop()
    case Failure(exception)  => throw exception; sparkSession.stop()
  }

}
