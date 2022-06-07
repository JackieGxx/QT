package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark10_coalesce {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls, 3)
    mk.mapPartitionsWithIndex(
      (index, num) => {
        println(index + "---->" + num.mkString(","))
        num
      }
    ).collect()

    println("==========1=============")
    //coalesce,默认不shuffle,主要是缩减分区
    val coa: RDD[Int] = mk.coalesce(2)

    coa.mapPartitionsWithIndex(
      (index, num) => {
        println(index + "---->" + num.mkString(","))
        num
      }
    ).collect()
    println("==========2=============")
    //repartition,主要是增加分区
    val rep: RDD[Int] = mk.repartition(5)
    rep.mapPartitionsWithIndex(
      (index, num) => {
        println(index + "---->" + num.mkString(","))
        num
      }
    ).collect()
    //关闭资源
    sc.stop()
  }
}
