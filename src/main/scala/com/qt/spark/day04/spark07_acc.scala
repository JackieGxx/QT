package com.qt.spark.day04

//行动算子

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object spark07_acc {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //    val ls = List(("a",1), ("a",2),("a",3) ,("a",4) ,("a",5))


    //2.makeRDD
    val nb: RDD[Int] = sc.makeRDD(List(2, 4, 6, 8, 10))

    //    val mk = sc.makeRDD(ls)
    val s: LongAccumulator = sc.longAccumulator

    //    mk.foreach{
    //      case (name,count)=>{
    //        s.add(count)
    //      }
    //    }
    nb.foreach(
      n => {
        s.add(n)
      }
    )
    println(s)
    println(s.value)
    //关闭资源
    sc.stop()
  }
}
