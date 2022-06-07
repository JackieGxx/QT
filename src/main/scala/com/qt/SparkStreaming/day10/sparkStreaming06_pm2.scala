//package com.qt.SparkStreaming.day10
//
///**
// * 需求二：统计各广告最近1小时内的点击量趋势，每6s更新一次（各广告最近1小时内各分钟的点击量）
// *   1.最近一个小时    窗口的长度为1小时
// *   2.每6s更新一次    窗口的滑动步长是6s
// *   3.各个广告每分钟的点击量 ((advId,hhmm),1)
// */
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//import org.apache.commons.codec.StringDecoder
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object sparkStreaming06_pm2 {
//  def main(args: Array[String]): Unit = {
//
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
//    val ssc = new StreamingContext(conf, Seconds(3))
//
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
//    //测试Kafka中消费数据   msg = 1584271384370,华南,广州,100,1
//    val dataDS: DStream[String] = kafkaDS.map(_._2)
//
//    //定义窗口
//    val windowDS: DStream[String] = dataDS.window(Seconds(12),Seconds(3))
//
//    //转换结构为 ((advId,hhmm),1)
//    val mapDS: DStream[((String, String), Int)] = windowDS.map {
//      line => {
//        val fields: Array[String] = line.split(",")
//        val ts: Long = fields(0).toLong
//        val day: Date = new Date(ts)
//        val sdf: SimpleDateFormat = new SimpleDateFormat("hh:mm")
//        val time = sdf.format(day)
//        ((fields(4), time), 1)
//      }
//    }
//
//    //对数据进行聚合
//    val resDS: DStream[((String, String), Int)] = mapDS.reduceByKey(_+_)
//    resDS.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//
//
//  }
//}
