import java.util
import java.util.Properties
import collection.JavaConversions._
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

object hoster{
  def main(args: Array[String]): Unit = {
    val hoster = new gatherer("Aceprune1", "oauth:xoe4xcxlup82tytfnmm68ae6rthf4o")
    hoster.joinChannel("#aceprune1")

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "test")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    props.put("session.timeout.ms", "30000")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String,String](props)
    consumer.subscribe(util.Arrays.asList("hostNewChannel"))

    while (true) {
      val records : ConsumerRecords[String, String] = consumer.poll(100)
      for (record: ConsumerRecord[String, String]  <- records){
        hoster.sendMessage("#aceprune1", ".host " + record.value().substring(1))
      }
    }
  }
}