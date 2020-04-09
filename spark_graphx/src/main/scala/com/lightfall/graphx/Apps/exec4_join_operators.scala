package com.lightfall.graphx.Apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object exec4_join_operators {
  def main(args: Array[String]): Unit = {
    // 创建 spark 上下文
    val spark_conf = new SparkConf().setAppName("MapOperator").setMaster("local[*]")
    val sc = new SparkContext(spark_conf)
    sc.setLogLevel("WARN")

    // 创建顶点 RDD
    val vertices_user: RDD[(VertexId, (String, Int))] = sc.parallelize(Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
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
    val default_vertex_user = ("Zero", 100)

    val graph = Graph(vertices_user, edges_relationship, default_vertex_user)

    println("join 前：")
    graph.vertices.collect.foreach(println)

    // 供 join 使用的 rdd, 单数 id 的顶点 age 加 1，反之不变
    val join_rdd: RDD[(VertexId, Boolean)] = sc.parallelize(Array((1L, true), (2L, false), (3L, true), (4L, false), (5L, true), (6L, false)))

    // 1. joinVertices 操作
    val joined_graph = graph.joinVertices(join_rdd)((vertex_id, attr, is_incr) => if(is_incr) (attr._1, attr._2+1) else (attr._1, attr._2))
    // 打印 join 完成后的结果
    println("join 后：")
    joined_graph.vertices.collect.foreach(println)

    // 2. outerJoinVertices 操作
    val outer_joined_graph = graph.outerJoinVertices(join_rdd){ (verterx_id, attr, is_incr) =>
      is_incr match {
        case Some(incr) => if(incr) (attr._1, attr._2+1) else (attr._1, attr._2)
        case None => (attr._1, 0)
      }
    }
    // 打印结果
    println("outerJoin 后：")
    outer_joined_graph.vertices.collect.foreach(println)
  }
}
