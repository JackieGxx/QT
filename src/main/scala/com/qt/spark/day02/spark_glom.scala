package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_glom {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls, 3)

    mk.mapPartitionsWithIndex(
      (index, ls) => {
        println(index + "--------->" + ls.mkString(","))
        ls
      }
    ).collect()
    println("=======================")
    //glom
    mk.glom().mapPartitionsWithIndex(
      (index, ls) => {
        println(index+"---------->"+ls.next().mkString(","))

        ls
      }
    ).collect()
    //关闭资源
    sc.stop()
  }
}
