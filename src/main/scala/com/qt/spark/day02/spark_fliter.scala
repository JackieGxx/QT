package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_fliter {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4, 5, 6, 7, 8,22)

    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls)
    mk.filter(_ % 2 == 0).collect().foreach(println)
    println("=======================")

    mk.filter(_ == 8).collect().foreach(println)
    println("=======================")

    mk.filter(_.toString.contains("2")).collect().foreach(println)

    println("=======================")

    sc.makeRDD(List("dt", "dtpapa", "love", "six", "dou"), 3).filter(_.contains("dt")).collect().foreach(println)

    println("=======================")
    mk.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
