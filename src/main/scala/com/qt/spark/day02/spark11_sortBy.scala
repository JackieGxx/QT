package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark11_sortBy {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4, -1, 6, -2, 9, -3)

    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls)
    mk.sortBy(num => num).collect().foreach(println)
    println("=======================")
    mk.sortBy(num => num, false).collect().foreach(println)
    println("=======================")
    val nb: RDD[String] = sc.makeRDD(List("18", "1", "2", "3", "22"))
    nb.sortBy(num => num).collect().foreach(println)
    println("=======================")
    nb.sortBy(_.toInt).collect().foreach(println)

    //关闭资源
    sc.stop()
  }
}
