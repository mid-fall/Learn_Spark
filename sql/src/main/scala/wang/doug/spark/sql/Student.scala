package wang.doug.spark.sql


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * 使用DataFrame API实现
  */
object Student {

  def main(args: Array[String]): Unit = {


    //创建sparkSession的实例，local[*]: 这种模式直接帮你按照Cpu最多Cores来设置线程数了。
    val spark = SparkSession.builder()
      .appName("Student")
      .master("local[*]")
      .getOrCreate()

    //创建RDD 1,zhangsan,M,22
    val lines: RDD[String] = spark.sparkContext.textFile("hdfs://hadoop01:8020/student")

    //转换RDD  RDD[String]-->RDD[Row]
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val sex = fields(2)
      val age = fields(3).toInt
      Row(id, name, sex, age)
    })

    //定义schema表头,定义顺序和rowRDD 一致
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("sex", StringType, true),
      StructField("age", IntegerType, true)
    ))

    //创建DataFrame，根据RowRDD 和schema
    val df: DataFrame = spark.createDataFrame(rowRDD, schema)

    df.show()

    //DataFrame API 实现

    //导入 隐式转换
   import spark.implicits._


    //女生 按照 年龄 和 ID 排序 male男生
    val df2: Dataset[Row] = df.select("id","name","age","sex").where($"sex" ==="M").orderBy($"age" desc,$"id" asc)


    df2.show()

    //男生 <=22
    val df3: Dataset[Row] = df2.select("id","name","age").where($"age" <= 22)

    df3.show()

    spark.stop()


  }
}
