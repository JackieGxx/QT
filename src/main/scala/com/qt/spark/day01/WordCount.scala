package com.qt.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val df = List("汤臣别墅")
    //读取外部文件
        val txRDD: RDD[String] = sc.textFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\input\\1.txt")
    //    val rkRDD: RDD[(String, Int)] = txRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //    sc.textFile(args(0)).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).saveAsTextFile(args(1))
    val red: RDD[(String, Int)] = txRDD.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //collect将执行的结果进行收集
    //    rkRDD.collect().foreach(println)
    red.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
