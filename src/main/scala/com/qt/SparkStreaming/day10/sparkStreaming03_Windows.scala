package com.qt.SparkStreaming.day10

/**
 * 4.1	无状态转化操作 transform
 */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreaming03_Windows {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202", 9999)
    val mpppDS: DStream[(String, Int)] = lineDStream.map((_, 1))
    //在Window函数中，最好只定义一次 窗口大小和 划窗大小
    val po: DStream[(String, Int)] = mpppDS.reduceByKeyAndWindow(_ + _, Seconds(3))


    val winDS: DStream[String] = lineDStream.window(Seconds(3), Seconds(9))
    val mpDS: DStream[(String, Int)] = winDS.map((_, 1))

    //    dsSort.print() 这个平时生产不会用，只用于测试
    //开启
    ssc.start()
    //关闭资源
    ssc.awaitTermination()

  }
}
