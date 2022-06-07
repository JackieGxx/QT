package com.qt.spark.test

import com.qt.spark.day04.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object hot_test {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //读取外部文件
    val tx: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\user_visit_action.txt")
    //映射需要用map
    val mapRDD: RDD[UserVisitAction] = tx.map( //映射需要用map
      lines => {
        val sp: Array[String] = lines.split("_")
        UserVisitAction(
          sp(0)
          , sp(1).toLong
          , sp(2)
          , sp(3).toLong
          , sp(4)
          , sp(5)
          , sp(6).toLong
          , sp(7).toLong
          , sp(8)
          , sp(9)
          , sp(10)
          , sp(11)
          , sp(12).toLong

        )
      }
    )
    //映射样例类
    val flatRDD: RDD[CategoryCountInfo] = mapRDD.flatMap(
      datas => {
        if (datas.click_category_id != -1) {
          List(CategoryCountInfo(datas.click_category_id.toString, 1, 0, 0))
        } else if (datas.pay_category_ids != "null") {
          val paycount: Array[String] = datas.pay_category_ids.split(",")
          var cifo = ListBuffer[CategoryCountInfo]()
          for (i <- paycount) {
            cifo.append(CategoryCountInfo(i, 0, 0, 1))
          }
          cifo
        } else if (datas.order_category_ids != "null") {
          val or: Array[String] = datas.order_category_ids.split(",")
          val co: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (a <- or) {
            co.append(CategoryCountInfo(a, 0, 1, 0))
          }
          co
        } else {
          Nil //空集合
        }
      }
    )
    val mp1: RDD[(String, (Long, Long, Long))] = flatRDD.map(
      line => {
        (line.categoryId, (line.clickCount, line.orderCount, line.payCount))
      }
    )
    //求和
    val sum: RDD[(String, (Long, Long, Long))] = mp1.reduceByKey(
      (a, b) => {
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      }
    )
    val mp2: RDD[(String, Long, Long, Long)] = sum.map(
      data => {
        (data._1, data._2._1, data._2._2, data._2._3)
      }
    )
    mp2.sortBy(
      line=>{
        (line._2,line._3,line._4)
      },false
    ).take(10).foreach(println)
    //关闭资源
    sc.stop()
  }
}
