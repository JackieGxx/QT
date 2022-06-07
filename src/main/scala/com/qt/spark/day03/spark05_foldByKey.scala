package com.qt.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

object spark05_foldByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(("a", 3), ("a", 11), ("b", 4), ("b", 12),("c", 8),("c", 99), ("a", 18), ("b", 16), ("c", 25))

    //2.makeRDD
    val mk = sc.makeRDD(ls, 2)
    mk.mapPartitionsWithIndex(
      (index, datas) => {
        println(index + "----->" + datas.mkString(","))
        datas
      }
    ).collect()
    println("=======================")
    mk.foldByKey(0)(_+_).collect().foreach(println)
    println("=======================")
    mk.foldByKey(10)(_+_).collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
