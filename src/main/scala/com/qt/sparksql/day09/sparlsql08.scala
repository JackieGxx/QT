package com.qt.sparksql.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object sparlsql08 {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象

    //创建入口
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    // 0 注册自定义聚合函数
    //    spark.udf.register("city_remark", new AreaClickUDAF)

    spark.sql("use sparkpractice")
    // 1. 查询出所有的点击记录,并和城市表产品表做内连接
    spark.sql(
      """
        |select
        |    c.*,
        |    v.click_product_id,
        |    p.product_name
        |from user_visit_action v
        |join city_info c
        |on v.city_id=c.city_id
        |join product_info p
        |on v.click_product_id=p.product_id
        |where click_product_id !=-1
      """.stripMargin).createOrReplaceTempView("t1")

    // 2. 计算每个区域, 每个产品的点击量
    spark.sql(
      """
        |select
        |    t1.area,
        |    t1.product_name,
        |    count(*) click_count,
        |    city_remark(t1.city_name)
        |from t1
        |group by t1.area, t1.product_name
      """.stripMargin).createOrReplaceTempView("t2")

    // 3. 对每个区域内产品的点击量进行倒序排列
    spark.sql(
      """
        |select
        |    *,
        |    rank() over(partition by t2.area order by t2.click_count desc) rank
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    // 4. 每个区域取top3

    spark.sql(
      """
        |select
        |    *
        |from t3
        |where rank<=3
      """.stripMargin).show

    //释放资源
    spark.stop()

  }
}






