package com.qt.spark.day04

//行动算子

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object spark10_actionCount {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val tx: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\user_visit_action.txt")
    //切分文本，和UserVisitAction样例类做映射
    val action: RDD[UserVisitAction] = tx.map(
      lines => {
        val sp: Array[String] = lines.split("_")
        UserVisitAction(
          sp(0),
          sp(1).toLong,
          sp(2),
          sp(3).toLong,
          sp(4),
          sp(5),
          sp(6).toLong,
          sp(7).toLong,
          sp(8),
          sp(9),
          sp(10),
          sp(11),
          sp(12).toLong
        )
      }
    )
    val ccf: RDD[CategoryCountInfo] = action.flatMap(
      datas => {
        //点击汇总
        if (datas.click_category_id != -1) {
          List(CategoryCountInfo(datas.click_category_id.toString, 1, 0, 0))
        } else if (datas.order_category_ids != "null") {
          val orderIds: Array[String] = datas.order_category_ids.split(",")
          //定义一个集合存放品牌ID封装的输出结果对象
          val cf = ListBuffer[CategoryCountInfo]()
          for (id <- orderIds) {
            cf.append(CategoryCountInfo(id, 0, 1, 0))
          }
          cf
        } else if (datas.pay_category_ids != "null") {
          //支付
          val payIds: Array[String] = datas.pay_category_ids.split(",")
          //定义一个集合
          val cf1: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (id <- payIds) {
            cf1.append(CategoryCountInfo(id, 0, 0, 1))
          }
          cf1
        } else {
          Nil
        }
      }
    )
    val tuples: RDD[(String, (Long, Long, Long))] = ccf.map(
      datas => {
        (datas.categoryId, (datas.clickCount, datas.orderCount, datas.payCount))
      }
    )
    val lon: RDD[(String, (Long, Long, Long))] = tuples.reduceByKey(
      (a, b) => {
        (a._1 + b._1, a._2 + b._2, a._3 + b._3)
      }
    )
    val pp: RDD[(String, Long, Long, Long)] = lon.map(
      datas => {
        (datas._1, datas._2._1, datas._2._2, datas._2._3)
      }
    )
    val sb: RDD[(String, Long, Long, Long)] = pp.sortBy(
      ds => {(ds._2,ds._3,ds._4)
      }, false
    )
    val t1: Array[(String, Long, Long, Long)] = sb.take(10)
    val ccinfo: Array[CategoryCountInfo] = t1.map(
      t => CategoryCountInfo(t._1, t._2, t._3, t._4)
    )
    ccinfo.foreach(println)
    println("-------------------")
    t1.foreach(println)

    //关闭资源
    sc.stop()
  }
}

//用户访问动作表
case class UserVisitAction(date: String, //用户点击行为的日期
                           user_id: Long, //用户的ID
                           session_id: String, //Session的ID
                           page_id: Long, //某个页面的ID
                           action_time: String, //动作的时间点
                           search_keyword: String, //用户搜索的关键词
                           click_category_id: Long, //某一个商品品类的ID
                           click_product_id: Long, //某一个商品的ID
                           order_category_ids: String, //一次订单中所有品类的ID集合
                           order_product_ids: String, //一次订单中所有商品的ID集合
                           pay_category_ids: String, //一次支付中所有品类的ID集合
                           pay_product_ids: String, //一次支付中所有商品的ID集合
                           city_id: Long) //城市 id
// 输出结果表
case class CategoryCountInfo(categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long) //支付次数
