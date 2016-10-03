import org.jibble.pircbot.PircBot
import java.io._
import java.net._
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class gatherer extends PircBot{
  var running = true
  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("group.id", "test")
  properties.put("enable.auto.commit", "true")
  properties.put("auto.commit.interval.ms", "1000")
  properties.put("session.timeout.ms", "30000")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val producer = new KafkaProducer[String,String](properties)

  def this(name: String, oauth : String) {
    this()
    setName(name)
    setVerbose(false)
    setEncoding("UTF-8")
    connect("irc.chat.twitch.tv", 6667, oauth)
  }


  override def onJoin(channel: String, sender:String, login:String, hostname:String){
  }

  override def onMessage(channel: String, sender: String, login: String, hostname: String, message: String): Unit ={
    producer.send(new ProducerRecord[String,String](channel.substring(1), channel,message))

  }

  override def onDisconnect(): Unit = {
    super.onDisconnect()
    producer.close()
  }



}

