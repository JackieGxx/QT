package com.qt.sparksql.day08

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object sparlsql02_udf {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象

    //创建入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark不是包名，是上下文环境对象名
    import spark.implicits._
    spark.udf.register("hi",(n:String)=>{"ni hao:"+n})
    //读取json文件,创建DataFrame
    val df: DataFrame = spark.read.json("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\input\\tt.josn")

    //查看df的数据
    //    df.show()
    /*
    sql风格
     */
    val dftemp: Unit = df.createOrReplaceTempView("dtlove")
    spark.sql("select hi(name),age  from dtlove").show()
    //    spark.sql("select * from dtlove").show()
    spark.stop()

  }

}

