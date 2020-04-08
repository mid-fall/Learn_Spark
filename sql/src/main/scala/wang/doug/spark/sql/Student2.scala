package wang.doug.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 使用SQL 方式实现
 */
object Student2 {

  def main(args: Array[String]): Unit = {


    val session = SparkSession.builder()
      .appName("Student2")
      .master("local[*]")
      .getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://hadoop01:8020/student")

    //
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val sex = fields(2)
      val age = fields(3).toInt
      Row(id, name, sex, age)
    })

    //定义schema表头
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("sex", StringType, true),
      StructField("age", IntegerType, true)
    ))

    //创建DataFrame
    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    df.createTempView("v_student")

    val df2: DataFrame = df.sqlContext.sql("select * from v_student")

    df2.show()


    val df3 = df.sqlContext.sql("select id,name,age from v_student where sex='M' order by age desc ")

    df3.show()

    session.stop()


  }
}
