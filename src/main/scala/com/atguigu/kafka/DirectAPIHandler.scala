package com.atguigu.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectAPIHandler {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("ReceiverWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
    val kafkaPara: Map[String, String] = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu"
    )
    val fromOffsets: Map[TopicAndPartition, Long] = Map[TopicAndPartition,Long](TopicAndPartition("atguigu",0)->50)
//    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](ssc,
//      kafkaPara,
//      fromOffsets,
//      (m: MessageAndMetadata[String, String]) => m.message())
    val kafkaDStream: InputDStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc, kafkaPara, fromOffsets, (m: MessageAndMetadata[String, String]) => m.message())
    var offsetRanges = Array.empty[OffsetRange]

    val wordToCountDStream: DStream[(String, Int)] = kafkaDStream.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)


      wordToCountDStream.foreachRDD(rdd => {
      for (o <- offsetRanges) {
        println(s"${o.topic}:${o.partition}:${o.fromOffset}:${o.untilOffset}")
      }
      rdd.foreach(println)
    })
    ssc.start()
    ssc.awaitTermination()

  }

}
