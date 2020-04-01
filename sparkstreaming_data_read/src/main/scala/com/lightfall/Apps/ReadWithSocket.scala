package com.lightfall.Apps

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReadWithSocket {
  def updateFunc(input:Seq[Int], result_sum:Option[Int]): Option[Int] = {
    val result: Int = input.sum + result_sum.getOrElse(0)
    Option(result)
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf().setMaster("local[*]").setAppName("ReadWithSocket")
    val sc = new StreamingContext(conf, Seconds(5))

    sc.checkpoint("hdfs://192.168.134.101:9000/ss_chkptr")
    val lines = sc.socketTextStream("192.168.134.101", 9999)
    val words = lines.flatMap(_.split(" ")).map(x => (x, 1))
    val counts = words.updateStateByKey(updateFunc)

    counts.print()
    sc.start()
    sc.awaitTermination()
  }
}
