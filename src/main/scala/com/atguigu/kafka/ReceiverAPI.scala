package com.atguigu.kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.ReceiverInputDStream
//import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

object ReceiverAPI {
  def main(args: Array[String]): Unit = {
    val sp: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ReceiverWordCount")
    val ssc = new StreamingContext(sp,Seconds(3))
   //KafkaUtils.createDirectStream()
    val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, "hadoop102:2181,hadoop103:2181,hadoop104:2181", "atguigu",
      Map[String, Int]("atguigu" -> 1)//1为分区数量
    )
    //维护在zookeeper中
    kafkaStream.map{
      case(_,value)=>(value,1)
    }.reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }

}
