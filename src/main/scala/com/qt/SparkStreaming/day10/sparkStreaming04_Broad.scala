package com.qt.SparkStreaming.day10

/**
 * 4.1	无状态转化操作 transform
 */

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.util.LongAccumulator

object sparkStreaming04_Broad {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
   import  spark.implicits._
    //创建StreamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    //创建DStream
    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop202", 9999)

    lineDStream.foreachRDD(
      lie=>{
        val df: DataFrame = lie.toDF()
        df.createOrReplaceTempView("user" )
        spark.sql("select * from user").show()
      }
    )
    //StreamingContext 转换成 sparkContext  ；ssc转换成sc
    val broadL: Broadcast[List[Int]] = ssc.sparkContext.broadcast(List(1, 2))
    val dtacc: LongAccumulator = ssc.sparkContext.longAccumulator("dt")

    //    dsSort.print() 这个平时生产不会用，只用于测试
    //开启
    ssc.start()
    //关闭资源
    ssc.awaitTermination()

  }
}
