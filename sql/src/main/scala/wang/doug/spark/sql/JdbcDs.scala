package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从JDBC读取数据
 */
object JdbcDs {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("JdbcDs")
      .master("local[*]")
      .getOrCreate()


    val user: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/spark",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user",
        "user" -> "root",
        "password" -> "root")
    ).load()

    user.printSchema();

    user.show()

    spark.stop()


  }

}
