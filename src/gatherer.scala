import org.jibble.pircbot.PircBot
import java.io._
import java.net._

import org.apache.kafka.clients.consumer.KafkaConsumer

class gatherer extends PircBot{
  var running = true
  def this(name: String, oauth : String) {
    this()
    setName(name)
    setVerbose(false)
    setEncoding("UTF-8")
    connect("irc.chat.twitch.tv", 6667, oauth)
    joinChannel("#aceprune1")
    run()

  }

  def run(): Unit ={
    while(running){
      Thread.sleep(5000)
    }
  }
  def stop(): Unit ={
    running = false
  }


  override def onJoin(channel: String, sender:String, login:String, hostname:String){

  }

  override def onMessage(channel: String, sender: String, login: String, hostname: String, message: String): Unit ={

  }



}

