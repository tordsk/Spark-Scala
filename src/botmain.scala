import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
//"oauth:xoe4xcxlup82tytfnmm68ae6rthf4o"
object botmain{
  def main(args: Array[String]): Unit = {
    val bot1Future= Future{
      val bot1 = new gatherer("Aceprune1", "oauth:xoe4xcxlup82tytfnmm68ae6rthf4o")
    }
    val bot2Future= Future{
      val bot2 = new gatherer("Aceprune1", "oauth:xoe4xcxlup82tytfnmm68ae6rthf4o")
    }

    while (true) {
      val input = scala.io.StdIn.readLine()
      if (input.equals("x")){
        Await.result(bot1Future,Duration.Zero)
        Await.result(bot2Future,Duration.Zero)
      }
    }
    Await.result(bot1Future,Duration.Inf)
  }
}