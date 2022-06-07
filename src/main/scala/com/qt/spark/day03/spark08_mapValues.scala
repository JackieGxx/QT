package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark08_mapValues {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List((1,"a"),(2,"b"), (3,"c"), (4,"d") )

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    val vrdd: RDD[(Int, String)] = mk.mapValues("--+--||" + _)
    println("=======================")
    vrdd.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
