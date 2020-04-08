package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object Write {


  def main(args: Array[String]): Unit = {

    //spark2.x SQL的编程API(SparkSession)
    //是spark2.x SQL执行的入口
    val spark = SparkSession.builder()
      .appName("Write")
      .master("local[*]")
      .getOrCreate()

    // 10亿
    val userCsv: DataFrame = spark.read.csv("C:/test/test.csv")

    userCsv.printSchema()

    userCsv.show()

    //

    val userDf: DataFrame = userCsv.toDF("id", "name", "sex", "age")


    userDf.printSchema()

    userDf.show()


    //写入csv  分析结果的数据， 25岁

    userDf.write.mode("overwrite").csv("C:/test/user_csv")


    //写入jdbc
    userDf.write.mode("overwrite").format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/spark",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "user",
        "user" -> "root",
        "password" -> "root")).save()

    //写入json
    userDf.write.mode("overwrite").json("C:/test/user_json");

    //写入txt
    userDf.select("name").write.mode("overwrite").text("C:/test/user_txt");

    userDf.select("name").write.mode("overwrite").text("hdfs://hadoop01:8020/spark_user");

    //userDf.show()

    spark.stop()


  }

}
