package com.lightfall.graphx.Apps

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object exec1_base {

  def main(args: Array[String]): Unit = {
    // 创建 spark 上下文
    val spark_conf = new SparkConf().setAppName("base").setMaster("local[*]")
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

    // 找出年龄大于 30 的顶点
    println("----------------------------------")
    println("年龄大于 30 的顶点：")
    graph.vertices.filter{case (vertex_id, (name, age)) => age > 30}.foreach(println)
    println("----------------------------------")

    // 找出属性大于 5 的边
    println("----------------------------------")
    println("属性大于 5 的边：")
    graph.edges.filter(edge =>
      edge.attr > 5).foreach(println)
    println("----------------------------------")

    // 找出最大入度、出度、度数

    // 先定义用于 reduce 的函数
    def MaxOf2Vertices(a: (VertexId, Int), b: (VertexId, Int)) = {
      if(a._2 > b._2) a else b
    }

    println("----------------------------------")
    // 最大入度
    val max_in_degree = graph.inDegrees.reduce(MaxOf2Vertices)._2
    println("最大入度为：" + max_in_degree)
    // 最大出度
    val max_out_degree = graph.outDegrees.reduce(MaxOf2Vertices)._2
    println("最大出度为：" + max_out_degree)
    // 最大度数
    val max_degree = graph.degrees.reduce(MaxOf2Vertices)._2
    println("最大度数为：" + max_degree)
    println("----------------------------------")

    // 终止程序
    sc.stop()
  }
}
