//package com.qt.SparkStreaming.day10
//
///**
// * 4.1	无状态转化操作 transform
// */
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.commons.codec.StringDecoder
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.spark.SparkConf
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.util.LongAccumulator
//
//object sparkStreaming05_prgam {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
//    val ssc = new StreamingContext(conf, Seconds(3))
//    //设置检查点
//    ssc.sparkContext.setCheckpointDir("D:\\dev\\workspace\\my-bak\\spark-realtime-bak\\cp")
//
//    //kafka参数声明
//    val brokers = "hadoop202:9092,hadoop203:9092,hadoop204:9092"
//    val topic = "my-ads-bak"
//    val group = "bigdata"
//    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
//    val kafkaParams = Map(
//      ConsumerConfig.GROUP_ID_CONFIG -> group,
//      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
//      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
//      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
//    )
//    //创建DS
//    val kafkaDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, Set(topic))
//
//    //测试Kafka中消费数据
//    val dataDS: DStream[String] = kafkaDS.map(_._2)
//
//    //打印输出
//    //dataDS.print()
//
//    //==========需求一实现： 每天每地区热门广告   msg = 1584271384370,华南,广州,100,1==========
//
//    /**
//     * 重点 公式 每天每地区 就是 groupby每天每地区（每天和每地区可以拼接成字符串）
//     */
//    //1.对获取到的Kafka中传递的原始数据
//    //2.将原始数据转换结构  (天_地区_广告,点击次数)
//    val mapDS = dataDS.map {
//      line => {
//        val fields = line.split(",")
//        //格式化时间戳
//        val timeStamp = fields(0).toLong
//        val day = new Date(timeStamp)
//        val sdf = new SimpleDateFormat("yyyy-MM-dd")
//        val dayStr = sdf.format(day)
//
//
//        ((dayStr + "_" + fields(1) + "_" + fields(4)), 1)
//      }
//    }
//    //3.将转换结构后的数据进行聚合处理 (天_地区_广告,点击次数sum)
//    //注意：这里要统计每天数据，所有要把每个采集周期的数据都统计，需要保存状态，使用updateStateByKey
//    val updateDS = mapDS.updateStateByKey(
//      (seq: Seq[Int], op: Option[Int]) => {
//        Option(op.getOrElse(0) + seq.sum)
//      }
//    )
//    //4.将聚合后的数据进行结构的转换   (天_地区,(广告,点击次数sum)))
//    val mapDS1: DStream[(String, (String, Int))] = updateDS.map {
//      case (k, sum) => {
//        val ks: Array[String] = k.split("_")
//        (ks(0) +"_" +ks(1),(ks(2), sum))
//      }
//    }
//    //5.按照天_地区对数据进行分组  (天_地区,Iterator[(广告,点击次数sum))])
//    val groupDS: DStream[(String, Iterable[(String, Int)])] = mapDS1.groupByKey()
//
//    //6.对分组后的数据降序取前三
//    val resDS: DStream[(String, List[(String, Int)])] = groupDS.mapValues {
//      datas => {
//        datas.toList.sortBy(-_._2).take(3)
//      }
//    }
//
//    resDS.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//  }
//}
