package com.lightfall.graphx.Apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

// GraphX 的 Map 操作练习

object exec2_map_operators {
  def main(args: Array[String]): Unit = {
    // 创建 spark 上下文
    val spark_conf = new SparkConf().setAppName("MapOperator").setMaster("local[*]")
    val sc = new SparkContext(spark_conf)

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

    // 利用 mapVertices 给顶点加一列职业
    // 判断年龄是否大于 30，来决定职业
    def GetOccupation(age: Int): String = {
      if(age > 30) "teacher" else "student"
    }
    // 得到新图
    val new_vertex_graph = graph.mapVertices{case (vertex_id, (name, age)) => (name, age, GetOccupation(age))}
    // 打印
    println("顶点新增职业属性：")
    new_vertex_graph.vertices.foreach(println)

    // 利用 mapEdges 给边加一列关系名称
    // 通过原关系值判断关系名称
    def GetRelationshipName(relationship_value: Int): String = {
      if(relationship_value == 0)
        "enemy"
      else if(relationship_value < 4)
        "stranger"
      else if(relationship_value < 9)
        "friend"
      else
        "couple"
    }
    // 得到新图
    val new_edge_graph = graph.mapEdges(edge => (edge.attr, GetRelationshipName(edge.attr)))
    // 打印
    println("边新增关系名称属性：")
    new_edge_graph.edges.foreach(println)

    // 三元组新增一列 String
    val new_triplet_graph = graph.mapTriplets(triplet => (triplet, "Made By Lightfall"))
    // 打印
    println("三元组新增属性：")
    new_triplet_graph.triplets.foreach(println)
  }
}
