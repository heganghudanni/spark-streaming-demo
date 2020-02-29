package com.atguigu.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SocketWordCount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SocketWordCount")

    //2.创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102",9999)
    val wordToCountDStream: DStream[(String, Int)] = lineDStream.transform(
      rdd => {
        val wordRDD: RDD[String] = rdd.flatMap(_.split(" "))
        val wordToOneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
        //省略形参
        val wordToCountRDD: RDD[(String, Int)] = wordToOneRDD.reduceByKey(_ + _)
        wordToCountRDD //
      }
    )
    wordToCountDStream.print()
    ssc.start()
    ssc.awaitTermination()


   }

}
