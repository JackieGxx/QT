package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark10_adcount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val adlog: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\agent.log")
    //切分 空格
    val sp: RDD[(String, Int)] = adlog.map(
      lines => {
        val datas: Array[String] = lines.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    )
    //次数相加
    val rd: RDD[(String, Int)] = sp.reduceByKey(_ + _)
    //map映射 split
    val ms: RDD[(String, (String, Int))] = rd.map {
      case (provicead, count) => {
        val addou: Array[String] = provicead.split("-")
        (addou(0), (addou(1), count))
      }
    }

    //分组
    val grp: RDD[(String, Iterable[(String, Int)])] = ms.groupByKey()
    //排序
    val mpv: RDD[(String, List[(String, Int)])] = grp.mapValues(
      al => {
        al.toList.sortWith {
          case ((pr1, ct1), (pr2, ct2)) => {
            ct1 > ct2
          }
        }
      }.take(3)
    )
    mpv.collect().foreach(println)
    println("=======================")
    val mvv: RDD[(String, List[(String, Int)])] = grp.mapValues(
      al => {
        al.toList.sortBy(_._2).reverse
      }.take(3)
    )
    mvv.collect().foreach(println)
    println("=======================")
    val mwv: RDD[(String, List[(String, Int)])] = grp.mapValues(
      al => {
        al.toList.sortBy(-_._2)
      }.take(3)
    )
    mwv.collect().foreach(println)

    //关闭资源
    sc.stop()
    }



}
