package com.qt.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object contains {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val arr1 = Array("1_2","2-3","3-4","4-5")
    val arr2 = Array("1_2","2-3","4-5")
    val arr4: Array[String] = arr1.filter (
      //  def contains[A1 >: A](elem: A1): Boolean = exists (_ == elem)
       arr1 => {
        arr2.contains(arr1)
      }
    )

    arr4.foreach(println)

    //关闭资源
    sc.stop()
  }
}
