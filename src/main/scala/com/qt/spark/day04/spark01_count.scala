package com.qt.spark.day04

//行动算子

import org.apache.spark.{SparkConf, SparkContext}

object spark01_count {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List[Int](26, 12, 88, 35, 100, (1), (2), (3), (4))
    val rdt: List[Int] = List[Int]()
    rdt.foreach(println)
    println("+++++++++++++++----------------+++++++++")

    ls.foreach(println)
    println("+++++++++++++++----------------+++++++++")

    val rt: Int = ls.count(_ > -1)
    println(ls)
    println("=======================")
    println(rt)
    //2.makeRDD
    val mk = sc.makeRDD(ls)
    //count
    println(mk.count())
    println("=======================")
    //first
    println(mk.first())
    println("=======================")
    //take
    val arr: Array[Int] = mk.take(3)
    println(arr.mkString(","))
    println("=======================")
    arr.foreach(println)
    //takeOrdered
    val tod: Array[Int] = mk.takeOrdered(3)
    println("=======================")
    tod.foreach(println)
    println("=======================")
    println(tod.mkString(","))
    //关闭资源
    sc.stop()
  }
}
