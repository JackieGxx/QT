package com.qt.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object spark01_partion {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val ls = List((1,2),(3,4),(5,6),(7,8))

    //2.makeRDD
    val mk = sc.makeRDD(ls,4)
    mk.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index+"---->"+datas.mkString(","))
        datas
      }
    ).collect()
    println("=======================")

    val hp: RDD[(Int, Int)] = mk.partitionBy(new MyPartitioner(3))
    hp.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index+"---->"+datas.mkString(","))
        datas
      }
    ).collect()
    //关闭资源
    sc.stop()
  }
}
//自定义分区器
class MyPartitioner (partitions: Int) extends Partitioner{
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = {
    2
  }
}