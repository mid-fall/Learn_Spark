package com.lightfall.graphx.Apps

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeContext, Graph}

object tmp {
    def sendMsg(ec:EdgeContext[Int,String,Int]):Unit = {
      ec.sendToDst( ec.srcAttr +1)
    }

    def mergeMsg(a: Int , b:Int) :Int = {
      math.max(a,b)
    }

    def sumEdgeCount( g:Graph[Int,String]):Graph[Int,String] = {
      val verts = g.aggregateMessages[Int]( sendMsg , mergeMsg)
      val g2 = Graph(verts ,g.edges)
      verts.foreach(println)
      println("verts-------------------------------------------------")
      g2.vertices.foreach(println)
      println("g2----------------------------------------------------")

      val check = g2.vertices.join(g.vertices).map( x => x._2._1 - x._2._2).reduce(_+_)

      if(check > 0)
        sumEdgeCount(g2)
      else
        g
    }



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

      val initGraph = myGraph.mapVertices((_,_)=>0)

      sumEdgeCount(initGraph).vertices.collect.foreach(println(_))
      println("result------------------------------------------------")



    }


}
