package com.lightfall.graphx.Apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object exec3_structural_operators {
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

    // 1. mask 练习
    // 创建一张图 2 使得图 2 中有些顶点和边在图 1 中不存在，调用图 1 的 mask 方法查看效果

    // 创建顶点 RDD
    val sub_vertices_user: RDD[(VertexId, Int)] = sc.parallelize(Array(
      (1L, 0),
      (2L, 0),
      (3L, 0),
      (4L, 0),
      (5L, 0),
      (6L, 0),
      (7L, 0)
    ))

    // 创建边 RDD
    val sub_edges_relationship: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      //Edge(3L, 2L, 4),
      //Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
      //Edge(1L, 7L, 5)
    ))

    val sub_default_vertex_user = (-1)

    val graph2 = Graph(sub_vertices_user, sub_edges_relationship, sub_default_vertex_user)

    // 打印结果
    val result_graph = graph.mask(graph2)
    println("----------------------------------------------------")
    println("mask 结果：")
    println("mask 结果顶点集：")
    result_graph.vertices.collect.foreach(println)
    println("mask 结果边集：")
    result_graph.edges.collect.foreach(println)
    println("----------------------------------------------------")

    // 2. subgraph 练习
    val valid_graph = graph.subgraph(vpred = (vertex_id, attr) => attr._2 > 40, epred = et => et.attr < 5)
    println("subgraph 结果：")
    println("subgraph 结果顶点集：")
    valid_graph.vertices.collect.foreach(println)
    println("subgraph 结果边集：")
    valid_graph.edges.collect.foreach(println)
    println("----------------------------------------------------")

    //3. reverse 练习
    val reverse_graph = graph.reverse
    println("reverse 结果：")
    println("reverse 结果顶点集：")
    reverse_graph.vertices.collect.foreach(println)
    println("reverse 结果边集：")
    reverse_graph.edges.collect.foreach(println)
    println("----------------------------------------------------")

    //4. groupEdges 练习
    // 创建一个有多重连接相同 id 顶点的边的图 graph3
    val group_vertices_user: RDD[(VertexId, Int)] = sc.parallelize(Array(
      (1L, 0),
      (2L, 0),
      (3L, 0)
    ))

    // 创建边 RDD
    val group_edges_relationship: RDD[Edge[Int]] = sc.parallelize(Array(
      Edge(1L, 2L, 3),
      Edge(1L, 2L, 4),
      Edge(1L, 2L, 5),
      Edge(2L, 3L, 3),
      Edge(2L, 3L, 4),
      Edge(1L, 3L, 2)
    ))

    val group_default_vertex_user = (-1)

    val graph3 = Graph(group_vertices_user, group_edges_relationship, group_default_vertex_user)
    val group_graph = graph3.groupEdges(merge = (left, right) => (left+right))

    // 打印结果
    println("groupEdges 结果：")
    println("groupEdges 结果顶点集：")
    group_graph.vertices.collect.foreach(println)
    println("groupEdges 结果边集：")
    group_graph.edges.collect.foreach(println)
    println("----------------------------------------------------")

    // 终止程序
    sc.stop()
  }
}
