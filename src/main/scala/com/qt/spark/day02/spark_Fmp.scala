package com.qt.spark.day02

/**
 * flatmap对集合进行扁平化
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_Fmp {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(List(1, 2), List(3, 4), List(5, 6), List(7, 8, 9), List(10))

    val mk = sc.makeRDD(ls, 3)

    mk.collect().foreach(println)
    println("========================")
    //1.flatmap
    val flmp: RDD[Int] = mk.flatMap(ls => ls)
    flmp.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
