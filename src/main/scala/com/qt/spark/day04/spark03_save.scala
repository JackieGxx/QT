package com.qt.spark.day04

//行动算子

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark03_save {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(26, 12, 88, 35, 100, 1, 2, 3, 4)

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    //saveAsTextFile
    //    mk.saveAsTextFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\output")
    //    //saveAsObjectFile 序列化文件
    //    mk.saveAsObjectFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\output1")
    //    //(K,v)格式
    //    val aa: Unit = mk.map((_, 1)).saveAsSequenceFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\output2")

    //    //
    //    val tx: RDD[String] = sc.textFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\output")
    //    println(tx)
    //    //
    val sq: RDD[(Int, Int)] = sc.sequenceFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\output2")
    sq.sortByKey().collect().foreach(println)
    //    //
    //    val ob: RDD[Nothing] = sc.objectFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\output1")
    //    ob.foreach(println)
    //关闭资源
    sc.stop()
  }
}
