package com.qt.sparksql.day09

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

object sparlsql01_MySQL {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象

    //创建入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    /*第一种
        val mysqlformat: DataFrame = spark.read.format("jdbc")
          .option("url", "jdbc:mysql://localhost:3306/myemployees")
          .option("driver", "com.mysql.jdbc.Driver")
          .option("user", "root")
          .option("password", "123456")
          .option("dbtable", "jobs")
          .load()
        mysqlformat.show()

     */
    /*第二种方式
        val df: DataFrame = spark.read.format("jdbc")
          .options(Map("url" -> "jdbc:mysql://localhost:3306/myemployees?user=root&password=123456",
    //        "user" -> "root",
    //        "password" -> "123456",可以把账号和密码拆开
            "driver" -> "com.mysql.jdbc.Driver",
            "dbtable" -> "jobs"
          )
          ).load()
        df.show()
    //      Driver驱动可以省略
     */

    /*第三种
        val po = new Properties()
        po.setProperty("user","root")
        po.setProperty("password","123456")
        val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/myemployees", "jobs", po)
        df.show
     */
    val rdd18: RDD[use] = spark.sparkContext.makeRDD(List(use("大咪咪", "汤臣别墅", "1547800208"), use("bigass", "江城逸品", "1388777778")))
    val ds: Dataset[use] = rdd18.toDS()
    ds.write.format("jdbc").
      option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option(JDBCOptions.JDBC_TABLE_NAME,"users")
      .mode("append")
//      .option("dbtable", "users")
//      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

}

case class use(name: String, address: String, phone: String)

