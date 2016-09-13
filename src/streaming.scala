import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD

/**
  * Created by tord on 08.09.16.
  */


object streaming {

  import org.apache.spark._
  import org.apache.spark.streaming._
  import org.apache.spark.streaming.kafka._



  def main(args: Array[String]): Unit = {
    val appName = "streamingspark"
    val master = "local[2]"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    val ssc = new StreamingContext(conf, Seconds(1))


    val props = new Properties
    props.put("bootstrap.servers", "localhost:9092")
    props.put("acks", "all")
    props.put("retries", "0")
    props.put("batch.size", "16384")
    props.put("linger.ms", "1")
    props.put("buffer.memory", "33554432")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")



    ssc.checkpoint("checkpoint")
    val memes = ssc.sparkContext.textFile("memes")
    val simplememes = memes.map(s=>s.toLowerCase()).collect()
    val memesbrodc = ssc.sparkContext.broadcast(simplememes.toSet)
    val topicmap = Map(("twitch_message",1))
    val lines = KafkaUtils.createStream(ssc,"localhost","spark",topicmap)
    val message = lines.map(_._2)
    val words = message.flatMap(s=>s.split(" "))

    val count = words.map(x => (x,1))
      .reduceByKeyAndWindow(_+_,_-_,Minutes(1),Seconds(3))

    val total = count.reduce((t1,t2) => ("tot", t1._2 + t2._2))
    val numberTotal = total.reduceByKey((i1,i2) => i1 + i2 )
    numberTotal.foreachRDD(rdd =>{
      val i = rdd.take(1)(0)._2
      val producer = new KafkaProducer[String, String](props)
      val message = new ProducerRecord[String, String]("twitch_results", null, "total," + i)
      producer.send(message)
      producer.close()
    })



    val emotesinchat = count.filter(msg => memesbrodc.value.contains(msg._1.toLowerCase)).persist()
      emotesinchat.reduce((t1,t2) => ("meme", t1._2 + t2._2))
        .foreachRDD(rdd =>{
        val i = rdd.take(1)(0)._2
        val producer = new KafkaProducer[String, String](props)
        val message = new ProducerRecord[String, String]("twitch_results", null, "totmeme," + i)
        producer.send(message)
        producer.close()
      })
    //count.print()

    emotesinchat.print()
    emotesinchat.foreachRDD(rdd =>
      rdd.foreachPartition{
        partOfRecords => {
          val producer = new KafkaProducer[String, String](props)
          partOfRecords.foreach(record =>  {
            if (record._2 > 0) {
              val message = new ProducerRecord[String, String]("twitch_results", null, record._1 +"," +  record._2)
              producer.send(message)

            }
        })
          producer.close()
        }
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }




}
