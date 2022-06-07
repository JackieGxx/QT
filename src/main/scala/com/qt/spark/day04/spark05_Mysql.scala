package com.qt.spark.day04

//json

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object spark05_Mysql {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    //数据库连接四要素
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/myemployees"
    val username = "root"
    val password = "123456"
    val sql = "select * from departments where department_id >= ? and department_id <= ?"
    val jd = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      },
      sql,
      10,
      100,
      2,
      res => (
        res.getInt(1), res.getString(2), res.getInt(3)
      )
    )
    jd.collect().foreach(println)

    //关闭资源
    sc.stop()
  }
}
