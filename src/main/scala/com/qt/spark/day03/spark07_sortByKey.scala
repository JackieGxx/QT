package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark07_sortByKey {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List((1, "a"), (2, "b"), (3, "c"), (4, "d"))

    val dtlopa: RDD[(dt, Int)] = sc.makeRDD(List((new dt(100, "牛掰"), 1),
      (new dt(80, "可以"), 1),
      (new dt(80, "凑合"), 1),
      (new dt(80, "还行"), 1),
      (new dt(62, "嗯呐"), 1),
      (new dt(35, "去你的吧"), 1)
    ))

    //2.makeRDD
    val mk = sc.makeRDD(ls)
    mk.sortByKey(false).collect().foreach(println)
    println("=======================")
    val di: RDD[(dt, Int)] = dtlopa.sortByKey(false)
    di.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}

class dt(var love: Int, var papa: String) extends Ordered[dt] with Serializable {
  override def compare(that: dt): Int = {
    var i: Int = this.love - that.love
    if (i == 0) {
      i = this.papa.compareTo(that.papa)
    }
    i
  }

  override def toString = s"dt($love, $papa)"
}
