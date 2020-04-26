package com.lightfall.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, types}

/**
 * 1. Spark SQL 基本 操作 将 下列 JSON 格式 数据 复制 到 Linux 系统 中， 并 保存 命名为 employee. json。
 * { "id": 1 , "name":" Ella" , "age": 36 }
 * { "id": 2, "name":" Bob"," age": 29 }
 * { "id": 3 , "name":" Jack"," age": 29 }
 * { "id": 4 , "name":" Jim"," age": 28 }
 * { "id": 4 , "name":" Jim"," age": 28 }
 * { "id": 5 , "name":" Damon" }
 * { "id": 5 , "name":" Damon" }
 *
 * 为 employee. json 创建 DataFrame， 并 写出 Scala 语句 完成 下列 操作：
 * （1） 查询 所有 数据；
 * （2） 查询 所有 数据， 并 去除 重复 的 数据；
 * （3） 查询 所有 数据， 打印 时 去除 id 字段；
 * （4） 筛选 出 age> 30 的 记录；
 * （5） 将 数据 按 age 分组；
 * （6） 将 数据 按 name 升序 排列；
 * （7） 取出 前 3 行 数据；
 * （8） 查询 所有 记录 的 name 列， 并为 其 取 别名 为 username；
 * （9） 查询 年龄 age 的 平均值；
 * （10） 查询 年龄 age 的 最小值。
 */

object practice1 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[*]").appName("spark_sql").getOrCreate()
        spark.sqlContext.sparkContext.setLogLevel("WARN")

        import spark.implicits._

        val df = spark.read.json("src/main/resources/employee.json")

        println("1. 查询所有数据")
        df.show()

        println("2. 查询所有数据，并去重复的数据")
        df.distinct().show()

        println("3. 查询所有数据，打印时去除 id 字段")
        df.select("name", "age").show()

        println("4. 筛选出 age> 30 的记录")
        df.filter($"age" > 30).show()

        println("5. 将数据按 age 分组")
        df.groupBy("age").count().show()

        println("6. 将数据按 name 升序排列")
        df.sort("name").show()

        println("7. 取出前 3 行数据")
        df.show(3)

        println("8. 查询所有记录的 name 列，并为其取别名为 username")
        df.select("name").withColumnRenamed("name", "username").show()

        println("9. 查询年龄 age 的平均值")
        df.agg("age"->"avg").show()

        println("10. 查询年龄 age 的最小值")
        df.agg("age"->"min").show()
    }
}
