package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark09_join {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List((1, "a"), (1, "p"), (2, "b"), (3, "c"), (4, "d"), (5, "op"))
    val lp = List((1, 10), (2, 12), (3, 15), (4, 16), (1, 9), (2, 88), (7, 35))

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    val mp: RDD[(Int, Int)] = sc.makeRDD(lp)
    mk.join(mp).collect().foreach(println)
    println("=======================")
    mk.cogroup(mp).collect().foreach(println)
    println("=======================")
    mp.cogroup(mk).collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
