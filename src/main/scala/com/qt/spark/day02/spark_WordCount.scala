package com.qt.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

object spark_WordCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List("dt love", "dt papa", "dt love", "dt forever")
    //2.makeRDD
    val mk = sc.makeRDD(ls, 2)



    /*简单版——1
    println("=============1==========")
    mk.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(
      (kv) => {
        (kv._1, kv._2.size)
      }
    ).collect().foreach(println)
    println("============2===========")

    mk.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map {
      case (word, ot) => {
        (word, ot.size)
      }
    }.collect().foreach(println)
    println("==========3=============")

    mk.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map {
      case (word, ot) => {
        (word, ot.map(k => {
          k._2
        }).sum
      }
    }.collect().foreach(println)
    println("==========4=============")



     */
    mk.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map {
      case (word, ot) => {
        (word, ot.map(_._2).sum)
      }
    }.collect().foreach(println)
    println("==========5=============")

    //简单版2
    mk.flatMap(_.split(" ")).groupBy(word => word).map(
      //提示中的对偶元组！！！
      kv => {
        (kv._1, kv._2.size)
      }
    ).collect().foreach(println)
    println("==========5=============")
    mk.flatMap(_.split(" ")).groupBy(word => word).map {
      case (k, v) => {
        (k, v.size)
      }
    }.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
