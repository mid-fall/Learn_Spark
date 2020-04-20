package com.lightfall.Apps

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReadWithHDFS {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val spark_conf = new SparkConf ().setAppName ("ReadWithHDFS").setMaster ("local[*]")
    val sc = new StreamingContext (spark_conf, Seconds (5) )

    val lines_dstream = sc.textFileStream ("hdfs://192.168.134.101:9000/stream_data")
    sc.checkpoint ("hdfs://192.168.134.101:9000/ss_chkptr")

    val words = lines_dstream.flatMap (_.split (" ") )
    val word_counts = words.map (x => (x, 1) ).reduceByKey (_+ _)

    word_counts.print ();

    sc.start ()
    sc.awaitTermination ()
  }
}
