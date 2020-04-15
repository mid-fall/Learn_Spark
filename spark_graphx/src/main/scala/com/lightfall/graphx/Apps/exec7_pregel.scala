package com.lightfall.graphx.Apps

import com.lightfall.graphx.Apps.exec6_aggregate_operators_2.sumEdgeCount
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

/**
 * 求图中顶点最大距离
 */
object exec7_pregel {
  def main(args: Array[String]): Unit = {
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    // 构建图
    val myVertices = sc.parallelize(Array((1L, "张三"), (2L, "李四"), (3L, "王五"), (4L, "钱六"),
      (5L, "领导")))
    val myEdges = sc.makeRDD(Array( Edge(1L,2L,"朋友"),
      Edge(2L,3L,"朋友") , Edge(3L,4L,"朋友"),
      Edge(4L,5L,"上下级"),Edge(3L,5L,"上下级")
    ))

    val myGraph = Graph(myVertices,myEdges)

    val g = myGraph.mapVertices((_,_)=>0)

    var res_g: Graph[Int, String] = g.pregel(0)(
      (vid, dist, A) => math.max(dist, A),
      triplet => {
        if (triplet.dstAttr < triplet.srcAttr + 1)
          Iterator((triplet.dstId, triplet.srcAttr + 1))
        else
          Iterator.empty
      },
      (a, b) => math.max(a, b)
    )

    println("result------------------------------------------------")
    res_g.vertices.collect.foreach(println(_))
    println("------------------------------------------------------")


  }
}
