package wang.doug.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * Spark 集成 Hive
 */
object Hive {


  def main(args: Array[String]): Unit = {

    //创建session，注意：开启hive支持
    val spark = SparkSession.builder()
      .appName("Hive")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()


    spark.sql("create database if not exists spark_demo")

    spark.sql("create table if not exists spark_demo.user (id int,name string)")

    spark.sql("insert into spark_demo.user values(1,'zhangsan')")

    spark.sql("insert into spark_demo.user values(2,'lisi')")

    val user = spark.sql("select count(*) from spark_demo.user ")

    user.printSchema()

    user.show()



    //写入JDBC MySQL


    spark.sql("drop table spark_demo.user ")


    spark.sql("drop database spark_demo ")


  }


}
