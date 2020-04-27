package com.lightfall.practice

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object practice3_jdbc {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder().master("local[2]")
            .appName("Spark SQL basic example")
            .getOrCreate()
        spark.sqlContext.sparkContext.setLogLevel("WARN")
        import spark.implicits._
        val connectionProperties = new Properties() ;
        val database_url = "jdbc:mysql://192.168.134.101:3306/sparktest"
        connectionProperties.put("user", "root")
        connectionProperties.put("password", "M-992212.Schuco")
        val jdbcDF: DataFrame = spark.read.jdbc(database_url, "employee", connectionProperties)
        jdbcDF.show()

        val employeeRDD: RDD[Array[String]] = spark.sparkContext.parallelize(Array("Mary F 26" , "Tom M 23")).map(_.split(" "))
        val rowRDD: RDD[Row] = employeeRDD.map(r=>Row(r(0),r(1),r(2).toInt))

        val schema = new StructType().add("name",StringType).add("gender",StringType).add("age",IntegerType)

        val employeeDF = spark.createDataFrame(rowRDD,schema)


        employeeDF.write.mode("append")
            .jdbc(database_url, "employee", connectionProperties)

        spark.close()
    }
}
