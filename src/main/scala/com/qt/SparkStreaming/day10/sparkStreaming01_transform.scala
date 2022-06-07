package com.qt.SparkStreaming.day10

/**
 * 4.1	无状态转化操作 transform
 */

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object sparkStreaming01_transform {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202", 9999)

    val dsSort: DStream[(String, Int)] = lineDStream.transform(
      line => {
        val mapRDD: RDD[(String, Int)] = line.map((_, 1))
        val sortRDD: RDD[(String, Int)] = mapRDD.sortByKey()
        sortRDD //最后一个是返回值，不能少
      }
    )
    //    dsSort.print() 这个平时生产不会用，只用于测试
    //开启
    ssc.start()
    //关闭资源
    ssc.awaitTermination()

  }
}
