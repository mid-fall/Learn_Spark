package com.lightfall.Apps

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, ConsumerStrategy, HasOffsetRanges, KafkaUtils, LocationStrategies, LocationStrategy, OffsetRange}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.lightfall.Utils.KafkaOffsetZKManager

/*
object ReadWithKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("kafkaSparkStreaming")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")
    // 获取上下文对象
    val ssc = new StreamingContext(context , Seconds(5))
    // 创建 LocationStrategy
    val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
    // 指明 Kafka 信息
    val brokers = "192.168.134.101:9092"
    val topic = "spark-kafka"
    val group = "sparkGroup"
    val zk_servers = "192.168.134.101:2181, 192.168.134.101:2182, 192.168.134.101:2183"
    /**
     * 参数说明
     * AUTO_OFFSET_RESET_CONFIG
     *     earliest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
     *     latest:当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
     *     none:topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */
    val kafkaParam = Map(
      "bootstrap.servers"-> brokers,
      "key.deserializer" ->classOf[StringDeserializer],
      "value.deserializer"->classOf[StringDeserializer],
      "group.id"->group,
      "auto.offset.reset"-> "latest",
      "enable.auto.commit" ->(false:java.lang.Boolean) // 禁止自动提交
    )
    // 创建 consumerStrategy
    //val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(Array(topic), kafkaParam)
    // 采用 zookeeper 手动维护偏移量
    val zkManager = new KafkaOffsetZKManager(zk_servers)
    val fromOffsets = zkManager.getFromOffset(Array(topic),group)
    //创建数据流
    var resultDStream:InputDStream[ConsumerRecord[String, String]] = null
    if (fromOffsets.size > 0){
      resultDStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        locationStrategy,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, fromOffsets)
      )
    }else{
      resultDStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        locationStrategy,
        ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam)
      )
      println("第一次消费 Topic:" + Array(topic))
    }

    //val resultDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, locationStrategy, consumerStrategy)

    // 处理数据流
    resultDStream.foreachRDD(iter=>{
      if(iter.count() > 0){
        iter.foreachPartition(partition_of_record => {
          partition_of_record.foreach(record => {
            val value: String = record.value()
            println(value)
          })
          // 手动维护偏移量
          val ranges: Array[OffsetRange] = partition_of_record.asInstanceOf[HasOffsetRanges].offsetRanges
          zkManager.storeOffsets(ranges, group)
        })
        //resultDStream.asInstanceOf[CanCommitOffsets].commitAsync(ranges)

        // 处理完数据保存/更新偏移量
     }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}
*/
