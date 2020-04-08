package com.lightfall.graphx.Apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

// GraphX 的 Map 操作练习

object exec2_map_operator {
  def main(args: Array[String]): Unit = {
    val spark_conf = new SparkConf().setAppName("MapOperator").setMaster("local[*]")
    val sc = new SparkContext(spark_conf)

    // 创建顶点 RDD
    val vertex_users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    //设置边 RDD
    val edge_relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

  }
}
