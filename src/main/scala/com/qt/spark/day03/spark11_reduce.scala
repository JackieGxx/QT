package com.qt.spark.day03

//行动算子

import org.apache.spark.{SparkConf, SparkContext}

object spark11_reduce {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List((1), (2), (3), (4))

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    //reduce
    println(mk.reduce(_ + _))
    println("=======================")
    //collect
    val col: Array[Int] = mk.collect()
    col.foreach(println)
    println("=======================")
    //foreach
    mk.foreach(println)
    //关闭资源
    sc.stop()
  }
}
