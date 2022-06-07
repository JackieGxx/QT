package com.qt.spark.day02

import org.apache.spark.{SparkConf, SparkContext}

object spark_HardWordCount {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List((("dt pa"), 2), (("dt love"), 3), (("dt like"), 4))
    //2.makeRDD
    val mk = sc.makeRDD(ls, 3)

    //复杂版1
//    mk.map {
//      case (w, c) => (w + " ") * c
//    }.flatMap(_.split(" ")).groupBy(w => w)
////    .map {
////      case (w, c) => (w, c.size)
////    }
//    .collect().foreach(println)


    //复杂版2
    mk.flatMap {
      case (w, c) =>
        w.split(" ").map(word => (word, c))
      //(f:String=>B),B是返回值
    }
      .groupBy(_._1) //(dt,CompactBuffer((dt,2), (dt,3), (dt,4))) 得出这样的结果
      .map {
        case (w, c) => (w, c.map(_._2).sum)
      }
      .collect().foreach(println)


    println("=======================")
    mk.collect().foreach(println)
    //关闭资源
    sc.stop()
  }
}
