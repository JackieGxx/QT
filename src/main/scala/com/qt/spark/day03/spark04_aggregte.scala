package com.qt.spark.day03

import org.apache.spark.{SparkConf, SparkContext}

object spark04_aggregte {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List(("a", 3), ("a", 11), ("b", 4),
      ("b", 12),("c", 8), ("c", 99),
      ("a", 18), ("b", 16), ("c", 25))

    //2.makeRDD
    val mk = sc.makeRDD(ls, 2)//2个分区，2*10
    mk.mapPartitionsWithIndex(
      (index, datas) => {
        println(index + "----->" + datas.mkString(","))
        datas
      }
    ).collect()
    println("=======================")
    mk.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)
    println("=======================")

    mk.aggregateByKey(0)(
      math.max(_, _), _ + _
    ).collect().foreach(println)
    println("=======================")
    mk.aggregateByKey(0)(
     (a,b)=>math.max(a,b), _ + _
    ).collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
