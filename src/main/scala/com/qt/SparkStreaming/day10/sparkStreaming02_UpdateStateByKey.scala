package com.qt.SparkStreaming.day10

/**
 * 4.1	无状态转化操作 transform
 */

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreaming02_UpdateStateByKey {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //    设置检查点路径  用于保存状态
    ssc.checkpoint("D:\\dev\\workspace\\my-bak\\spark-bak\\cp")

    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202", 9999)


    val mpDs: DStream[(String, Int)] = lineDStream.map((_, 1))
    val upDS: DStream[(String, Int)] = mpDs.updateStateByKey(
      (sq: Seq[Int], op: Option[Int]) => {
        Option(sq.sum + op.getOrElse(0))
      }
    )
    //    dsSort.print() 这个平时生产不会用，只用于测试
    //开启
    ssc.start()
    //关闭资源
    ssc.awaitTermination()

  }
}
