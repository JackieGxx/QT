package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_groupBy {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls, 2)

    mk.mapPartitionsWithIndex(
      (index, ls) => {
        println(index + "-------------->" + ls.mkString(","))
        ls
      }
    ).collect()
    println("=======================")
    val gb: RDD[(Int, Iterable[Int])] = mk.groupBy(_ % 3)
    gb.mapPartitionsWithIndex(
      (index, ls) => {
        println(index + "-------------->" + ls.mkString(","))
        ls
      }
    ).collect()
    gb.collect().foreach(println)
    //睡会儿
    Thread.sleep(100000)
    //关闭资源
    sc.stop()
  }
}
