package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读取CSV的数据源
 */
object CsvDs {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("CsvDs")
      .master("local[*]")
      .getOrCreate()

    val userCsv: DataFrame = spark.read.csv("C:/test/user.csv")

    userCsv.printSchema()

    userCsv.show()


    val userDf: DataFrame = userCsv.toDF("id", "name", "sex", "age")


    //userDf.printSchema()

    //userDf.show()

    spark.stop()


  }

}
