package com.qt.spark.day04

//行动算子

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object spark11_TopN_req2{
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
          val cf: ListBuffer[CategoryCountInfo] = ListBuffer[CategoryCountInfo]()
          for (id <- payIds) {
            cf.append(CategoryCountInfo(id, 0, 0, 1))
          }
          cf
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
      ds => {
        (ds._2, ds._3, ds._4)
      }, false
    )
    val t1: Array[(String, Long, Long, Long)] = sb.take(10)
    val ccinfo: Array[CategoryCountInfo] = t1.map(
      t => CategoryCountInfo(t._1, t._2, t._3, t._4)
    )
    //    ---------------------------------------
    //    ---------------------------------------
    //    ---------------------------------------
    //1.获取前10的品类ID
    val tenIdS: Array[String] = ccinfo.map(_.categoryId)
    val bro: Broadcast[Array[String]] = sc.broadcast(tenIdS)
    //2.从源文件中进行过滤
    val fl: RDD[UserVisitAction] = action.filter(
      user => {
        if (user.click_category_id != -1) {
          bro.value.contains(user.click_category_id.toString)
        } else {
          false
        }
      }
    )

    //3.取3个元素，再进行映射
    val senn: RDD[(String, Int)] = fl.map(
      datas => {
        ((datas.click_category_id + "_" + datas.session_id), 1)
      }
    )
    //4.reduceByKey进行聚合
    val redK: RDD[(String, Int)] = senn.reduceByKey(_ + _)
    //5.再进行map
    val css: RDD[(String, (String, Int))] = redK.map {
      case (clickAndsen, num) => {
        val cs: Array[String] = clickAndsen.split("_")
        (cs(0), (cs(1), num))
      }
    }

    //6.分组
    val grp: RDD[(String, Iterable[(String, Int)])] = css.groupByKey()
    //7.排序  groupByKey()和mapValues(中的sortWith)进行内部排序
    val op = grp.mapValues(
      datas => {
        datas.toList.sortWith(
          (l, r) => {
            l._2 > r._2
          }
        ).take(10)
      }
    )
        op.collect().foreach(println)





    //关闭资源
    sc.stop()
  }
}

