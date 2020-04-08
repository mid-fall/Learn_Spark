package wang.doug.spark.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ScoreLevel {


  def main(args: Array[String]): Unit = {

    //spark2.x SQL的编程API(SparkSession)
    //是spark2.x SQL执行的入口
    val spark = SparkSession.builder()
      .appName("StudentScore")
      .master("local[*]")
      .getOrCreate()

    //创建RDD
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

    }).toDF("id", "name", "sex", "age")

    stuDataFrame.createTempView("v_student")

    //创建RDD
    val scores: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/score")

    val scoreDataFrame: DataFrame = scores.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val studentId = fields(1).toLong
      val course = fields(2)
      val score = fields(3).toInt
      (id, studentId, course, score)

    }).toDF("id", "studentId", "course", "score")

    scoreDataFrame.createTempView("v_score")

    //注册函数

    spark.udf.register("scoreLevel", (score: Int) => {
      if (score >= 90)
        "A"
      else if (score >= 80)
        "B"
      else if (score >= 70)
        "C"
      else if (score >= 60)
        "D"
      else
        "E"

    })


    val stuScoreDataFrame: DataFrame = spark.sql("select  v_score.id,v_student.name,v_score.course,v_score.score,scoreLevel(v_score.score) as level from v_score left join v_student on v_student.id=v_score.studentId ")

    stuScoreDataFrame.show()

    spark.stop()


  }

}
