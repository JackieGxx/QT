package com.qt.spark.day04

//json

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object spark04_Json {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val js: RDD[String] = sc.textFile("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\input\\tt.josn")
    val jp: RDD[Option[Any]] = js.map(JSON.parseFull)
    jp.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
