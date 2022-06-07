package com.qt.spark.day04

//行动算子

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_aggfold {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(26, 12, 88, 35, 100, (1), (2), (3), (4))
    val rt: Int = ls.count(_ > -1)
    println("=======================")
    println(rt)
    //2.makeRDD
    val mk = sc.makeRDD(ls, 3)//3个分区，再加上分区内的初始值，3*10+10
    val mkp: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (3, "v"), (1, "f"), (2, "p"), (3, "r"), (2, "w"), (1, "a")))
    //aggregate
    println(mk.aggregate(0)(_ + _, _ + _))
    println(mk.aggregate(10)(_ + _, _ + _))
    println("=======================")
    //fold
    println(mk.fold(0)(_ + _))
    println(mk.fold(10)(_ + _))
    println("=======================")
    //countByKey
    val i: collection.Map[Int, Long] = mkp.countByKey()
    println(i)

    //关闭资源
    sc.stop()
  }
}
