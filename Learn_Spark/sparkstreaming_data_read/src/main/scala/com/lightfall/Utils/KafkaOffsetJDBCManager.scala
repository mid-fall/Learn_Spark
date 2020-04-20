package com.lightfall.Utils

import java.sql.{DriverManager, ResultSet}

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import scala.collection.mutable

/**
  手动维护 offset 的工具类
  首先在 MySQL 创建如下表：
  CREATE TABLE `kfaka_offsets` (
     `topic` varchar(255) NOT NULL,
     `partition` int(11) NOT NULL,
     `groupid` varchar(255) NOT NULL,
     `offset` bigint(20) DEFAULT NULL,
     PRIMARY KEY (`topic`,`partition`,`groupid`)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 */

object KafkaOffsetJDBCManager {
  val jdbc_url = "jdbc:mysql://192.168.134.101/spark_kafka"

  // 将偏移量保存到数据库
  def saveOffsets(offset_range: Array[OffsetRange], group_name: String) = {
    val conn = DriverManager.getConnection(jdbc_url, "root", "M-992212.Schuco")
    val statement = "replace into kafka_offsets (`topic`, `partition`, `groupid`, `offset`) values(?,?,?,?)"
    val prep = conn.prepareStatement(statement)

    for(offset <- offset_range){
      try{
        prep.setString(1, offset.topic)
        prep.setInt(2, offset.partition)
        prep.setString(3, group_name)
        prep.setLong(4, offset.untilOffset)
        prep.executeUpdate()
      }
    }
    prep.close()
    conn.close()
  }

  // 从数据库读取偏移量
  def getOffsetMap(topics: Array[String], group_name: String) ={
    val conn = DriverManager.getConnection(jdbc_url, "root", "M-992212.Schuco")
    val statement = "select * from kafka_offsets where groupid=? and topic=?"
    val prep = conn.prepareStatement(statement)

    // 暂时先只写只有一个 topic 的情况
    val topic = topics(0)
    prep.setString(1, group_name)
    prep.setString(2, topic)
    val res: ResultSet = prep.executeQuery()
    val offset_map = mutable.Map[TopicPartition, Long]()
    while (res.next()) {
      offset_map += new TopicPartition(res.getString("topic"), res.getInt("partition")) -> res.getLong("offset")
    }

    res.close()
    prep.close()
    conn.close()
    offset_map
  }
}
