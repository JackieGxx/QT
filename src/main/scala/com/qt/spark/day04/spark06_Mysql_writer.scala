package com.qt.spark.day04

//json

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark06_Mysql_writer {
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

    val na: RDD[(Int, String)] = sc.makeRDD(List((888, "dt"), (300, "dna")))
    na.foreachPartition {
      datas => {
        //注册驱动
        Class.forName(driver)
        //获取连接
        val conn: Connection = DriverManager.getConnection(url, username, password)
        //声明数据库操作的语句
        val sql: String = "insert into departments(department_id,department_name) values(?,?)"
        //创建数据库操作对象
        val ps: PreparedStatement = conn.prepareStatement(sql)
        //对当前分区内数据，进行遍历

        datas.foreach {
          case (id, name) => {
            //给参数赋值
            ps.setInt(1, id)
            ps.setString(2, name)
            //执行sql
            ps.executeUpdate()
          }
        }
        //关闭连接
        ps.close()
        conn.close()
      }
    }
    
    //关闭资源
    sc.stop()
  }
}
