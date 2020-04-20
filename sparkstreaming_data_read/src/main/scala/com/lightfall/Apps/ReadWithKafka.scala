package com.lightfall.Apps

import com.lightfall.Utils.KafkaOffsetJDBCManager
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable
import com.lightfall.Utils.KafkaOffsetZKManager

object ReadWithKafka2 {
  def main(args: Array[String]): Unit = {
    // 1. 获取上下文对象
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkaSparkStreaming2")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")
    val ssc = new StreamingContext(context , Seconds(5))
    // 指明一些 Kafka 信息
    val brokers = "192.168.134.101:9092"
    val topics = Array("spark-kafka")
    val group = "sparkGroup"
    /**
     * 参数说明
     * auto.offset.reset：
     *     earliest: 当各分区下有已提交的 offset 时，从提交的 offset 开始消费；无提交的 offset 时，从头开始消费
     *     latest: 当各分区下有已提交的 offset 时，从提交的 offset 开始消费；无提交的 offset 时，消费新产生的该分区下的数据
     *     none: topic 各分区都存在已提交的 offset 时，从 offset 后开始消费；只要有一个分区不存在已提交的 offset，则抛出异常
     */
    val kafkaParam = Map(
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset"-> "latest",
      "enable.auto.commit" -> (false:java.lang.Boolean) // 禁止自动提交
    )

    // 2. 创建数据流
    // 创建 LocationStrategy
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
    // 创建 consumerStrategy
    //val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(topics, kafkaParam)
    // 如果 MySQL 中没有 offset 记录，则从 latest 处开始消费
    // 如果 MySQL 中有 offset 记录，则从 offset 出开始消费
    //val resultDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)
    val offset_map = KafkaOffsetJDBCManager.getOffsetMap(topics, group)
    var resultDStream: InputDStream[ConsumerRecord[String, String]] = null
    if(offset_map.size > 0) {
      println("MySQL 中有 offset 记录，从该 offset 处开始消费")
      resultDStream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam, offset_map))
    } else {
      println("MySQL 中没有 offset 记录，从 latest 开始消费")
      resultDStream = KafkaUtils.createDirectStream[String, String](ssc, locationStrategy,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParam))
    }

    // 3. 处理数据流
    resultDStream.foreachRDD(iter=>{
      if(iter.count() > 0){
        iter.foreach(record =>{
          val value : String = record.value()
          println(value)
        })
        // 手动维护偏移量
        val ranges: Array[OffsetRange] = iter.asInstanceOf[HasOffsetRanges].offsetRanges
        // 处理完数据保存/更新偏移量
        //resultDStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)
        KafkaOffsetJDBCManager.saveOffsets(ranges, group)
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}