
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SimpleGraphX {

  def main(args: Array[String]): Unit = {

    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //设置users顶点
    val users: RDD[(VertexId, (String, Int))] =
      sc.parallelize(Array(
        (1L, ("Alice", 28)),
        (2L, ("Bob", 27)),
        (3L, ("Charlie", 65)),
        (4L, ("David", 42)),
        (5L, ("Ed", 55)),
        (6L, ("Fran", 50))
      )
      )

    //设置relationships边
    val relationships: RDD[Edge[Int]] =
      sc.parallelize(Array(
        Edge(2L, 1L, 7),
        Edge(2L, 4L, 2),
        Edge(3L, 2L, 4),
        Edge(3L, 6L, 3),
        Edge(4L, 1L, 1),
        Edge(5L, 2L, 2),
        Edge(5L, 3L, 8),
        Edge(5L, 6L, 3)
      )
      )


    // 创建图对象
    // Build the initial Graph
    val graph = Graph(users, relationships)

    // 找出年龄大于 30 的顶点
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    // 找出属性大于 5 的边
    graph.edges.filter(e=>e.attr>5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    // 找出图中最大的出度、入度

    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
      if (a._2 > b._2) a else b
    }

    var tuples: Array[(VertexId, Int)] = graph.outDegrees.collect()
    tuples.foreach(println(_))

    graph.edges.collect.foreach(println(_))

    println("max of outDegrees:" + graph.outDegrees.reduce(max) + " max of inDegrees:" + graph.inDegrees.reduce(max) + " max of Degrees:" + graph.degrees.reduce(max))



  }

}
