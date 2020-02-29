package com.atguigu.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateWordCount02 {

  def getSSC: _root_.org.apache.spark.streaming.StreamingContext = {

    //1.创建SparkConf对象
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SocketWordCount")

    //2.创建StreamingContext对象
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //设置CK
    ssc.checkpoint("./update03")//多个任务不能用同一个checkpoint

    //3.读取端口数据创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //4.转换为一个个的单词
    val wordDStream: DStream[String] = lineDStream.flatMap(_.split(" "))

    //5.转换为元组
    val wordToOneDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    val updateFunc: (Seq[Int], Option[Int]) => Some[Int] = (seq: Seq[Int], state: Option[Int]) => {
      //当前批次针对Key的计算结果
      val sum: Int = seq.sum
      //上一次计算的结果
      val lastSum: Int = state.getOrElse(0)
      //返回值
      Some(sum + lastSum)
    }

    //6.使用有状态转换计算累加的WordCount
    val wordToCountDSteam: DStream[(String, Int)] = wordToOneDStream.updateStateByKey(updateFunc)

    //7.打印
    wordToCountDSteam.print()

    ssc
  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./update03", () => getSSC)
//无法做累加:::

    ssc.start()
    ssc.awaitTermination()

  }

}
