package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_doubleVaule {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val rdd1 = List(1, 2, 3, 4, 5, 6)
    val rdd2 = List(5, 6, 7, 8, 9, 10)

    //2.makeRDD
    val rd1: RDD[Int] = sc.makeRDD(rdd1)
    val rd2: RDD[Int] = sc.makeRDD(rdd2)

    //合集 union
    rd1.union(rd2).collect().foreach(println)
    println("=======================")
    //并集 intersection
    rd1.intersection(rd2).collect().foreach(println)
    println("=======================")
    //差集 subtract
    rd1.subtract(rd2).collect().foreach(println)
    println("=======================")
    rd2.subtract(rd1).collect().foreach(println)
    println("=======================")
    //拉链 zip  分区数和分区内的元素数量必须一样
    rd1.zip(rd2).collect().foreach(println)

    //关闭资源
    sc.stop()
  }
}
