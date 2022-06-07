package com.qt.sparksql.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object sparlsql03_udaf {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建入口
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //spark不是包名，是上下文环境对象名
    import spark.implicits._
    //读取json文件,创建DataFrame
    val df: DataFrame = spark.read.json("C:\\Users\\胡丹婷\\IdeaProjects\\spark-suc\\input\\tt.josn")
    val mg = new myavg
    spark.udf.register("mmavg", mg)
    /*
    sqld风格适合DF
     */
    //创建临时视图
    df.createOrReplaceTempView("uu")
    spark.sql("select mmavg(age) from uu").show()
    //关闭资源
    spark.stop()
  }
}

class myavg extends UserDefinedAggregateFunction {
  // 聚合函数输入参数的数据类型
  override def inputSchema: StructType = {
    StructType(Array(StructField("age", IntegerType)))
  }

  // 聚合函数缓冲区中值的数据类型(age,count)
  override def bufferSchema: StructType = {
    StructType(Array(StructField("SUM", LongType), StructField("COUNT", LongType)))
  }

  // 函数返回值的数据类型
  override def dataType: DataType = DoubleType

  // 稳定性：对于相同的输入是否一直返回相同的输出
  override def deterministic: Boolean = true

  // 函数缓冲区初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //    存年龄的总和
    buffer(0) = 0L
    // 存年龄的个数
    buffer(1) = 0L

  }

  // 更新缓冲区中的数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      //从row中get值时, 为空问题
      //get数值类型:如果为空, 则转化为0
      //判空, 建议使用row.isNullAt(index)
      //
      //get字符串类型:如果为空则为null
      //判空建议用:StringUtils.isEmpty() (出现""的情况)
      //
      //总结:获取值用getAsT
      //判空用row.isNullAt
      //以及StringUtils.isEmpty()

      buffer(0) = buffer.getLong(0) + input.getInt(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  // 合并缓冲区
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // 计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble / buffer.getLong(1)
  }
}

