package com.lightfall.graphx.Apps

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * eg1：统计粉丝年龄
 */
object exec5_aggregate_operators {
  def main(args: Array[String]): Unit = {
    // 创建 spark 上下文
    val spark_conf = new SparkConf().setAppName("base").setMaster("local[*]")
    val sc = new SparkContext(spark_conf)
    sc.setLogLevel("WARN")

    // 创建顶点 RDD
    val vertices_user: RDD[(VertexId, Double)] = sc.parallelize(Array(
      (1L, 20.0),
      (2L, 27.0),
      (3L, 65.0),
      (4L, 42.0),
      (5L, 55.0),
      (6L, 30.0)
    ))

    // 创建边 RDD
    val edges_relationship: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    ))

    // 定义默认顶点
    // 当边中使用了不存在的顶点时，会使用这个默认顶点
    val default_vertex_user = (0.0)

    val graph = Graph(vertices_user, edges_relationship, default_vertex_user)

    // 定义 SendMsg 方法
    // 如果 dst 的 age 大于 src 的 age，就代表是粉丝，将它的年龄发送给 dst
    def SendMsg(edge_context: EdgeContext[Double, Int, (Int, Double)]): Unit = {
      if(edge_context.dstAttr > edge_context.srcAttr) {
        edge_context.sendToDst((1, edge_context.srcAttr))
      }
    }

    // 使用 aggregateMessages 方法来统计粉丝数
    val older_followers: VertexRDD[(Int, Double)] = graph.aggregateMessages[(Int, Double)](SendMsg, (a, b) => (a._1+b._1, a._2+b._2))

    val avarage_followers = older_followers.map{case (vertex_id, (num, age)) => (age/num)}

    avarage_followers.collect.foreach(println)

    sc.stop()
  }
}

