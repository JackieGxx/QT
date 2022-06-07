package com.qt.spark.day04

//行动算子

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object spark12_TopN_req3 {
  def main(args: Array[String]): Unit = {

    //创建SparkConf配置文件
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DT")
    //创建SparkContext对象
    val sc = new SparkContext(conf)
    val tx: RDD[String] = sc.textFile("E:\\BaiduNetdiskDownload\\user_visit_action.txt")
    //切分文本，和UserVisitAction样例类做映射
    val action: RDD[UserVisitAction] = tx.map(
      lines => {
        val sp: Array[String] = lines.split("_")
        UserVisitAction(
          sp(0),
          sp(1).toLong,
          sp(2),
          sp(3).toLong,
          sp(4),
          sp(5),
          sp(6).toLong,
          sp(7).toLong,
          sp(8),
          sp(9),
          sp(10),
          sp(11),
          sp(12).toLong
        )
      }
    )
    /*
    求分母
     */
    //映射
    val mp: RDD[(Long, Long)] = action.map(
      datas => {
        (datas.page_id, 1L)
      }
    )
    //reduceByKey 求和分母
    val rdk: RDD[(Long, Long)] = mp.reduceByKey(_ + _)
    val mmp: Map[Long, Long] = rdk.collect().toMap
    /*
    求分子
     */
    //同一用户 session_id进行操作
    val ssgp: RDD[(String, Iterable[UserVisitAction])] = action.groupBy(_.session_id)
    //同一用户 session_id进行操作
    val sns: RDD[(String, List[(String, Int)])] = ssgp.mapValues(
      user => {
        //通过时间进行排序
        val timeSort: List[UserVisitAction] = user.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        //对排序后的结构进行转换，只保留page_id
        val pd: List[Long] = timeSort.map(_.page_id)
        //拉链匹配
        val zp: List[(Long, Long)] = pd.zip(pd.tail)
        //最后映射
        zp.map(
          lg => {
            (lg._1 + "-" + lg._2, 1)
          }
        )
      }
    )
    val fp: RDD[(String, Int)] = sns.map(_._2).flatMap(ls => ls)//RDD一般不是List集合
    val fzcount: RDD[(String, Int)] = fp.reduceByKey(_ + _)
    fzcount.foreach{//遍历一下，把（A->B页面的跳转情况，累积计数），一个一个的遍历出来，这样才可以进行除法计算
      case (pl, fzsum) => {
        val sp: Array[String] = pl.split("-")
        val fmpg: Long = sp(0).toLong
        //获取分母的值,MAP的（k,v)类型
        val fmsum: Long = mmp.getOrElse(fmpg, 1L)
        //计算转化率
        println(pl+"->"+ fzsum.toDouble / fmsum)
//        val str: String = pl + "->" + fzsum.toDouble / fmsum
      }
    }






    //关闭资源
    sc.stop()
  }
}

