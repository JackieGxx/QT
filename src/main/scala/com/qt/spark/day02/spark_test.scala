package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_test {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(("dt",18),("nana",19),("mimi",22))

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    val co: Long = mk.count()
    val age: RDD[Int] = mk.map(
      a => {
        a._2
      }
    )
    val we: RDD[Int] = mk.map(_._2)
    println(we.sum() / co)
    println(age.sum() / co)
    println("=======================")
    mk.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
