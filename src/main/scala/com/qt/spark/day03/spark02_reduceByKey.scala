package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark02_reduceByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(("a",1),("b",2),("c",3),("a",3),("b",4),("c",8))

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    val rk: RDD[(String, Int)] = mk.reduceByKey(_ + _)
    rk.collect().foreach(println)
    println("=======================")
    //关闭资源
    sc.stop()
  }
}
