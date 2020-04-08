import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapGraphX {

  def main(args: Array[String]): Unit = {
    //设置运行环境
    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //设置users顶点
    val users: RDD[(VertexId, (String, Int))] =
      sc.parallelize(Array((3L, ("rxin", 23)), (7L, ("jgonzal", 34)), (5L, ("franklin", 45)), (2L, ("istoica", 65))))

    //设置relationships边
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 10L, "collab"),Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    // 定义默认的作者,以防与不存在的作者有relationship边
    val defaultUser = ("John Doe", 0)

    // Build the initial Graph
    val graph: Graph[(String, Int), String] = Graph(users, relationships, defaultUser)

    graph.vertices.collect.foreach(println(_))

    var graph2: Graph[(String, Int), String] = graph.mapVertices((vid: VertexId, attr: (String, Int)) => (attr._1, 2 * attr._2))
    println("-------------------------")
    graph2.vertices.collect.foreach(println(_))

    graph.vertices.collect.foreach(println(_))
  }

}
