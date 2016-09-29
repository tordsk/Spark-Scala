import java.util.Properties
import javax.swing.JList

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.Json
import org.json4s.native.JsonMethods._

import scala.io.Source

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
    val simplememes = memes.map(s => s.toLowerCase()).collect()
    val memesbrodc = ssc.sparkContext.broadcast(simplememes.toSet)
    val topics = ssc.sparkContext.textFile("../channels")
    val topicbrodc = ssc.sparkContext.broadcast(topics.map(s => (s.toLowerCase(), 1)).collect().toMap)
    val zkQuorum = "localhost:2181"
    val group = "client"


    print(topicbrodc.value)
    getChannelList(props)

    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicbrodc.value).window(Seconds(30), Minutes(30))
    val message = lines.map(s => (s._1, s._2.toLowerCase()))
    val memelines = message.filter(msg => msg._2.split(" ").toSet.intersect(memesbrodc.value).nonEmpty)

    val totalMemesPrChannel = memelines.map(s => (s._1,1))
      .reduceByKey((c,c1) => c + c1)
    totalMemesPrChannel.print()
    val topchannel = totalMemesPrChannel.reduce((r,r1) => {
      if (r._2 > r1._2)
      r
      else
        r1
    })
    topchannel.foreachRDD(rdd =>{
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val message = new ProducerRecord[String, String]("hostnew", null, record._1)
          producer.send(message)
        })
        producer.close()
      })
    })
    totalMemesPrChannel.foreachRDD(rdd =>{
      rdd.foreachPartition(partition => {
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val message = new ProducerRecord[String, String](record._1.substring(1) + "_results", null, "TotalMemes,"+record._2)
          producer.send(message)
        })
        producer.close()
      })
    })

    message.map(t=> (t._1, 1))
        .reduceByKey((i,i1) => i+i1)
          .foreachRDD(rdd =>{
            rdd.foreachPartition(partition => {
              val producer = new KafkaProducer[String, String](props)
              partition.foreach(record => {
                val message = new ProducerRecord[String, String](record._1.substring(1) + "_results", null, "TotalMsg,"+record._2)
                producer.send(message)
              })
              producer.close()
            })
          })

      val memespresent = memelines.flatMap(s=> {
        val memeset = s._2.split(" ").toSet.intersect(memesbrodc.value)
        memeset.map(a=> s._1 ->a).toMap})
      .map(s=>(s,1))
      .reduceByKey((i,i1) => i + i1)
      val topmemeprchannel = memespresent.map(s=>(s._1._1,(s._1._2,s._2)))
      .reduceByKey((t,t1) => {
        if (t._2 > t1._2)
           t

        else
           t1
        })




    topchannel.print()
    topmemeprchannel.print()
    ssc.start()
    ssc.awaitTermination()


  }

  def getChannelList(props : Properties){
    val producer = new KafkaProducer[String, String](props)

    val fruit: List[String] = List()
    val json = scala.io.Source.fromURL("https://api.twitch.tv/kraken/streams?client_id=88edxnb08ogpinb3fp9n8cox9nm5u98")

    val parsed = parse(json.mkString)
    val channels = parsed \\ "channel"
    val names = channels \\ "display_name"
    val listofnames = names.children

    listofnames.foreach(s => {
      print("JOINING " + s.values.toString)
      val message = new ProducerRecord[String, String]("JoinChannel", null, s.values.toString)
      producer.send(message)

    })

    producer.close()
  }
}

