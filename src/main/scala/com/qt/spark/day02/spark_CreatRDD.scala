package com.qt.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark_CreatRDD {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(1, 2, 3, 4)
    val mpp = List(("a",1), ("b",3), ("c",5), ("d",7))
    val mpppl: Map[String, Int] = mpp.toMap
    val df= mpppl.get("b").get
    mpppl.foreach(println)
    println(df)
    val i: Int = mpppl.getOrElse("a", 0)
    println(i)

//    //1.parallelize 集合方式创建（内存）
//    val para: RDD[Int] = sc.parallelize(ls)
    //2.makeRDD
    val mk: RDD[Int] = sc.makeRDD(ls)
//    para.collect().foreach(println)
//    println("=======================")
//    mk.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
