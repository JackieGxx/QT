package com.qt.sparksql.day08

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object sparlsql01_demo {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象

    //创建入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    //spark不是包名，是上下文环境对象名
    //读取json文件,创建DataFrame
    val df: DataFrame = spark.read.json("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\input\\tt.josn")
    //    查看df的数据
    //        df.show()
    /*
    sql风格
     */
    //    val dftemp: Unit = df.createOrReplaceTempView("dtlove")
    //    spark.sql("select * from dtlove").show()
    /*
    DSL风格
     */
    //        df.select("name","age").show()
    //        df.select("*").show()
    //           println("=============================")
    //            df.select($"name", $"age" + 1).show()
    //            println("=============================")
    //            df.filter($"age" > 18).show()
    /*
    DataFrame-->RDD
     */
    //        val dfTordd: RDD[Row] = df.rdd
    //        dfTordd.foreach(println)
    /*
    RDD-->DataFrame
    RDD-->DataSet
     */


    val rdd1: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((60, "dt", 17), (70, "nana", 19)))
    val rdd2: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((60, "dt", 20), (70, "nana", 21)))
    val NC: RDD[nice] = rdd2.map(
      a => nice(a._1, a._2, a._3)
    )
    //
    //
    val df1: DataFrame = rdd1.toDF("weight","name","age")
    //        df1.show()
    //        NC.toDS().show()
    val ds: Dataset[nice] = df1.as[nice]
    ds.show()

    spark.stop()
  }

}

case class nice(weight: Int, name: String, age: Int)
