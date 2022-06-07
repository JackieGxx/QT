package com.qt.spark.day04

//行动算子

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object spark08_acc_self {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List("dt", "love", "dtai", "dd", "dt", "dtlove")
    //2.makeRDD

    val mk = sc.makeRDD(ls)
    //创建累加器对象
    val mac = new Myacc
    //注册累加器
    sc.register(mac)
    //使用累加器
    mk.foreach(
      w => {
        mac.add(w)
      }
    )
    //输出累加器
    println(mac.value)





    //关闭资源
    sc.stop()
  }
}

class Myacc extends AccumulatorV2[String, mutable.Map[String, Int]] {

  var mp = mutable.Map[String, Int]()

  //是否为初始值
  override def isZero: Boolean = mp.isEmpty

  //拷贝
  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val newAcc = new Myacc
    newAcc.mp = this.mp
    newAcc
  }

  //重置
  override def reset(): Unit = mp.clear()

  //向累加器中添加元素
  override def add(v: String): Unit = {
    if (v.startsWith("d")) {
      mp(v) = mp.getOrElse(v, 0) + 1
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    //当前Excutor的map
    var m1 = mp
    //另个一个Exctor的map
    var m2 = other.value

    mp = m1.foldLeft(m2) {
      //mm表示m2,kv表示m1
      (mm, kv) => {
        val k: String = kv._1
        val v: Int = kv._2
        mm(k) = mm.getOrElse(k, 0) + v
        mm

      }
    }


  }

  //获取累加器的值
  override def value: mutable.Map[String, Int] = mp
}
