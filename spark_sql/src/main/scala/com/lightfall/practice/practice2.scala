package com.lightfall.practice

import org.apache.spark.sql.SparkSession

/**
 * 2. 编程实现将 RDD 转换为 DataFrame 源文件，内容如下（包含 id, name, age）：
 * 1, Ella, 36
 * 2, Bob, 29
 * 3, Jack, 29
 * 请将数据复制保存到 Linux 系统 中， 命名为 employee.txt， 实现从 RDD 转换得
 * 到 DataFrame，并按 “id: 1, name: Ella, age: 36” 的格式打印出 DataFrame 的所有
 * 数据。 请写出程序代码。
 */

object practice2 {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder().master("local[*]").appName("spark_sql").getOrCreate()
        spark.sqlContext.sparkContext.setLogLevel("WARN")

        import spark.implicits._
        val text_file = spark.sparkContext.textFile("src/main/resources/employee.txt")
        val df = text_file.map(line => {
            val arr = line.split(",")
            (arr(0).toInt, arr(1), arr(2).toInt)
        }).toDF("id", "name", "age")

        df.show()
    }
}
