package com.lightfall.graphx.Apps

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object exec8_page_rank {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("page_rank").setMaster("local[*]");
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val graph = GraphLoader.edgeListFile(sc, "src/main/resources/followers.txt")
    val ranks = graph.pageRank(0.0001).vertices

    var users: RDD[(Long, String)] = sc.textFile("src/main/resources/users.txt").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    val rank_by_username = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    println(rank_by_username.collect.mkString("\n"))

    sc.stop()
  }
}
