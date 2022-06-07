package com.qt.spark.test

import com.qt.spark.day04.UserVisitAction
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object trans_test {
  def main(args: Array[String]): Unit = {
    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("WC")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //读取外部文件
    val tx: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\user_visit_action.txt")
    //映射需要用map
    val mapRDD1: RDD[UserVisitAction] = tx.map( //映射需要用map
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
    //求分母
    val pg: RDD[(Long, Int)] = mapRDD1.map(line => {
      (line.page_id, 1)
    }
    )
    val fmSum: RDD[(Long, Int)] = pg.reduceByKey(_ + _)
    val fmSumMap: Map[Long, Int] = fmSum.collect().toMap //先转换为行动算子，变成集合，再变成Map
    //求分子,需要是同一个session_id，这样才可以算出访问的路径
    val senn1: RDD[(String, Iterable[UserVisitAction])] = mapRDD1.groupBy(_.session_id)
    //
    val pgso: RDD[(String, List[(Long, Long)])] = senn1.mapValues(
      line => {
        //时间排序

        val timeSort: List[UserVisitAction] = line.toList.sortWith(
          (a, b) => {
            a.action_time < b.action_time
          }
        )


        val pgSort: List[Long] = timeSort.map(_.page_id)

        val pgZip: List[(Long, Long)] = pgSort.zip(pgSort.tail)

        pgZip
      }
    )
    val pgList: RDD[List[(Long, Long)]] = pgso.map(_._2)

    val pgToFlat: RDD[(Long, Long)] = pgList.flatMap(line => line)

    val pgConut: RDD[((Long, Long), Int)] = pgToFlat.map(line => {
      (line, 1)
    })
    val fzSum: RDD[((Long, Long), Int)] = pgConut.reduceByKey(_ + _)


    println("=====================")
    fzSum.foreach(
      line => {
        val fm = fmSumMap.getOrElse(line._1._1, 1)
        val fz = line._2
        val rate = fz.toDouble / fm
        println(line._1._1 + "->" + line._1._2 + "=" + rate)
      }
    )





    //关闭资源
    sc.stop()
  }
}
