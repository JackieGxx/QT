package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_sample {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = (1 to 10)
    //1.parallelize 集合方式创建（内存）

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    mk.sample(true,1).collect().foreach(println)
    println("=======================")

    mk.sample(false,0.5).collect().foreach(println)
    println("=======================")
    mk.take(3).foreach(println)
    println("=======================")
    //takeSample，直接返回数组
    mk.takeSample(false,3).foreach(println)


    //关闭资源
    sc.stop()
  }
}
