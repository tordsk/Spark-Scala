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
    val simplememes = memes.map(s => s.toLowerCase()).collect()
    val memesbrodc = ssc.sparkContext.broadcast(simplememes.toSet)
    val topics = ssc.sparkContext.textFile("channels")
    val topicbrodc = ssc.sparkContext.broadcast(topics.map(s => (s, 1)).collect().toMap)
    val zkQuorum = "localhost:2181"
    val group = "client"
    print(topicbrodc.value)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicbrodc.value).window(Minutes(1), Seconds(5))


    val message = lines.map(s => (s._1, s._2.toLowerCase()))
    val memelines = message.filter(msg => msg._2.split(" ").toSet.intersect(memesbrodc.value).nonEmpty)
    val totalMemesPrChannel = memelines.map(s => (s._1,1))
      .reduceByKey((c,c1) => c + c1)
    totalMemesPrChannel.print()

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



    memespresent.print()
    topmemeprchannel.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

    /*
    val count = words.map(x => (x,1))
      .reduceByKeyAndWindow(_+_,_-_,Minutes(1),Seconds(3))

    val total = count.reduce((t1,t2) => (t1._1, t1._2 + t2._2))
    val numberTotal = total.reduceByKey((i1,i2) => i1 + i2 )
    numberTotal.foreachRDD(rdd =>{
      val i = rdd.take(1)(0)._2
      val producer = new KafkaProducer[String, String](props)
      val message = new ProducerRecord[String, String]("twitch_results", null, "total," + i)
      producer.send(message)
      producer.close()
    })



    val emotesinchat = count.filter(msg => memesbrodc.value.contains(msg._1)).persist()
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






*/

