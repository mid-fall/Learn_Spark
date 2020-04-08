package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 使用DataFrame 的API实现
 */
object StudentScore2 {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("StudentScore2")
      .master("local[*]")
      .getOrCreate()

    //创建Dataset
    val students: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/student")

    import spark.implicits._
    //
    val stuDataFrame: DataFrame = students.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val sex = fields(2)
      val age = fields(3).toInt
      (id, name, sex, age)

    }).toDF("studentId", "name", "sex", "age")

    stuDataFrame.createTempView("v_student")


    //创建Dataset
    val scores: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/score")


    val scoreDataFrame: DataFrame = scores.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val studentId = fields(1).toLong
      val course = fields(2)
      val score = fields(3).toInt
      (id, studentId, course, score)

    }).toDF("scoreId", "stuId", "course", "score")

    scoreDataFrame.createTempView("v_score")


    val scoreStuDataFrame: DataFrame = scoreDataFrame.join(stuDataFrame, $"stuId" === $"studentId", "left");


    scoreStuDataFrame.show(10)

    val scoreStuDataFrame2: DataFrame = scoreStuDataFrame.select("name", "course", "score").where("score >=60").orderBy($"score" desc);



    scoreStuDataFrame2.show(10)


    spark.stop()


  }
}
