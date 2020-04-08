package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object WordCount {


  def main(args: Array[String]): Unit = {


    var master = "local[*]"
    if (args.length > 0 && args(0).equals("remote")) {
      master = "spark://hadoop01:7077"
    }

    //创建SparkSession
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master(master)
      .getOrCreate()


    val lines: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/wc")

    lines.show()


    //导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))


    words.show()


    words.createTempView("v_wordcount")


    val result: DataFrame = spark.sql("SELECT value as word, COUNT(*) as counts FROM v_wordcount GROUP BY word ORDER BY counts DESC")


    result.show()


    spark.stop()

  }

}
