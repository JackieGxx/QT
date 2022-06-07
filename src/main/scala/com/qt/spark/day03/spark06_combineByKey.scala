package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark06_combineByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(("a", 3), ("a", 11), ("b", 4), ("b", 12), ("c", 8), ("c", 99), ("a", 18), ("b", 16), ("c", 25))

    //2.makeRDD
    val mk = sc.makeRDD(ls, 2)
    mk.mapPartitionsWithIndex(
      (index, datas) => {
        println(index + "----->" + datas.mkString(","))
        datas
      }
    ).collect()
    println("=======================")
    val mp: RDD[(String, (Int, Int))] = mk.map {
      case (cl, num) => {
        (cl, (num, 1))
      }
    }
    mp.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    ).collect().foreach(println)
    println("=======================")
    //复杂/复杂版
    mk.map {
      case (cl, num) => {
        (cl, (num, 1))
      }
    }.reduceByKey {
      case ((score1, count1), (score2, count2)) => {
        (score1 + score2, count1 + count2)
      }
    }.map {
      case (cl, sc) => {
        (cl, sc._1 / sc._2.toDouble)
      }
    }.collect().foreach(println)

    println("=======================")
    //combineByKey 版
    //1.	作用: 针对每个K, 将V进行合并成C, 得到RDD[(K,C)]
    val cm: RDD[(String, (Int, Int))] = mk.combineByKey(
      (_, 1),
      (t1: (Int, Int), v) => {
        (t1._1 + v, t1._2 + 1)
      },
      (t2: (Int, Int), t3: (Int, Int)) => {
        (t2._1 + t3._1, t2._2 + t3._2)
      }
    )
    cm.map{
      case (name,(score,count))=>(name,score/count)
    }.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
