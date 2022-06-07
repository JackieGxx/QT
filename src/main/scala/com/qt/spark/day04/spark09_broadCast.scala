package com.qt.spark.day04

//行动算子

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark09_broadCast {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(("a", 1), ("b", 2), ("c", 3))
    val mk: RDD[(String, Int)] = sc.makeRDD(List(("a", 7), ("b", 8), ("c", 9)))
    val br: Broadcast[List[(String, Int)]] = sc.broadcast(ls)
    val kk: RDD[(String, (Int, Int))] = mk.map {
      case (k1, v1) => {
        var v3 = 0
        for ((k2, v2) <- br.value) {
          if (k1 == k2) {
            v3 = v2
          }
        }
        (k1, (v1, v3))
      }
    }
    kk.collect().foreach(println)

    //关闭资源
    sc.stop()
  }
}
