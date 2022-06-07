package com.qt.sparksql.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

object sparlsql04_udaf {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark不是包名，是上下文环境对象名
    import spark.implicits._
    //读取json文件,创建DataFrame
    val df: DataFrame = spark.read.json("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\input\\tt.josn")
    //封装为DataSet
    val ds: Dataset[User01] = df.as[User01]

    //创建聚合函数
    var myAgeUdtf1 = new MyAveragUDAF1
    //将聚合函数转换为查询的列
    val col: TypedColumn[User01, Double] = myAgeUdtf1.toColumn

    ds.select(col).show()


    //关闭资源
    spark.stop()
  }
}

//输入数据类型
case class User01(name: String, age: Long)

//缓存类型
case class AgeBuffer(var sum: Long, var count: Long)

/**
 * 定义类继承org.apache.spark.sql.expressions.Aggregator
 * 重写类中的方法
 */
class MyAveragUDAF1 extends Aggregator[User01, AgeBuffer, Double] {
  override def zero: AgeBuffer = {
    AgeBuffer(0L, 0L)
  }

  override def reduce(b: AgeBuffer, a: User01): AgeBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }

  override def finish(buff: AgeBuffer): Double = {
    buff.sum.toDouble / buff.count
  }

  //DataSet默认额编解码器，用于序列化，固定写法
  //自定义类型就是produce   自带类型根据类型选择
  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}

