package com.atguigu.receiver
import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

class MyReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  //接收数据的方法
  def receive(): Unit = {
    try {
      //创建Socket对象
      val socket = new Socket(host, port)

      //创建流
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))

      //读取数据
      var input: String = reader.readLine()

      while (input != null && !isStopped()) {

        //写入Spark内存
        store(input)

        //读取新的数据
        input = reader.readLine()
      }

      //出现异常情况
      reader.close()
      socket.close()
      restart("重启！")
    } catch {
      case e: Exception => restart("重启！")
      case t: Throwable => restart("重启！")
    }
  }

  //启动时调用的方法
  override def onStart(): Unit = {
    //开启新的线程读取数据
    new Thread() {
      override def run(): Unit = receive()
    }.start()
  }

  //关闭时调用的方法
  override def onStop(): Unit = {

  }
}
