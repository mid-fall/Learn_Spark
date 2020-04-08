package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从JSON中读取数据
 */
object JsonDs {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("JsonDs")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    //指定以后读取json类型的数据(有表头)
    val user: DataFrame = spark.read.json("C:/test/user.json")

    user.show()

    //DataFrame API 实现
    val filterUser: DataFrame = user.where($"age" <= 22).orderBy($"id" asc)
    filterUser.show()

    spark.stop()


  }

}
