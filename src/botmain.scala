import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.JsonAST.JString
import org.json4s.native.JsonMethods._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
//"oauth:xoe4xcxlup82tytfnmm68ae6rthf4o"
object botmain{

  def main(args: Array[String]): Unit = {
      val list = List[Future[Unit]]()
      val json = scala.io.Source.fromURL("https://api.twitch.tv/kraken/streams?client_id=88edxnb08ogpinb3fp9n8cox9nm5u98")
      val parsed = parse(json.mkString)
      val channels = parsed \\ "channel"
      val names = channels \\ "name"
      val listofnames = names.children
      val newbotchannelist = listofnames.slice(0,4)
      val bot1Future = Future{
        val bot1 = new gatherer("Aceprune1", "oauth:xoe4xcxlup82tytfnmm68ae6rthf4o")
        print(" Started new Bot @ ")
        for (channel <- newbotchannelist){
          print(channel.values.toString)
          channel match {
            case JString(s) => bot1.joinChannel("#" + s)
            case _ => print("ERROR WHILE PARSING CHANNEL STRING")
          }
        }

    }
    val secondChannelist = listofnames.slice(4,8)
    val bot2Future = Future{
      val bot1 = new gatherer("Aceprune1", "oauth:xoe4xcxlup82tytfnmm68ae6rthf4o")
      print(" Started new Bot @ ")
      for (channel <- secondChannelist){
        print(channel.values.toString)
        channel match {
          case JString(s) => bot1.joinChannel("#" + s)
          case _ => print("ERROR WHILE PARSING CHANNEL STRING")
        }
      }

    }

    val thirdChannelList = listofnames.slice(8,11)
    val bot3Future = Future{
      val bot1 = new gatherer("Aceprune1", "oauth:xoe4xcxlup82tytfnmm68ae6rthf4o")
      print(" Started new Bot @ ")
      for (channel <- thirdChannelList){
        print(channel.values.toString)
        channel match {
          case JString(s) => bot1.joinChannel("#" + s)
          case _ => print("ERROR WHILE PARSING CHANNEL STRING")
        }
      }

    }

    var running = true
    while (running) {
      val input = scala.io.StdIn.readLine()
      if (input.equals("x")){
        Await.result(bot1Future,Duration.Zero)
        Await.result(bot2Future,Duration.Zero)
        Await.result(bot3Future,Duration.Zero)
        running = false
      }
    }
    System.exit(0)
  }
}