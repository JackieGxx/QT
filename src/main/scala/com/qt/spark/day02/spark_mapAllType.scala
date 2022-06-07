package com.qt.spark.day02

/**
 * map映射的所有类型
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_mapAllType {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val mk: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    //1.map
    val m: RDD[Int] = mk.map(_ * 2)
    //2.mapPartitions
    val mp: RDD[Int] = mk.mapPartitions(ls => {
      ls.map(_ * 2)
    })
    //3.mapPartitionsWithIndex
    val mpi: RDD[Int] = mk.mapPartitionsWithIndex((index, ls) => {
      index match {
        case 1 => ls.map(_ * 2)
        case _ => ls

      }
    })

    val mif: RDD[Int] = mk.mapPartitionsWithIndex(
      (in, ls) => {
        if (in == 1) {
          ls.map(_ * 2)
        }
        else ls
      }
    )
    mk.collect().foreach(println)
    println("=========================")
    m.collect().foreach(println)
    println("=========================")
    mp.collect().foreach(println)
    println("=========================")
    mpi.collect().foreach(println)
    println("=========================")
    mif.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
