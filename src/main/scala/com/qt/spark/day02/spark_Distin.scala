package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_Distin {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4, 4, 5, 3, 2, 1, 5, 6, 2)

    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls, 5)
    mk.mapPartitionsWithIndex {
      (index, num) => {
        println(index + "--->" + num.mkString(","))
        num
      }
    }.collect()


    println("=======================")

    val dk: RDD[Int] = mk.distinct(2)
    dk.mapPartitionsWithIndex {
      (index, num) => {
        println(index + "--->" + num.mkString(","))
        num
      }
    }.collect()
    //    mk.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
