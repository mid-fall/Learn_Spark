package wang.doug.spark.sql


import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * 通过学生表关联分数表
 * 使用Spark的2.x的API实现
 * 使用SQL方式实现
 *
 */
object StudentScore {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder()
      .appName("StudentScore")
      .master("local[*]")
      .getOrCreate()

    //创建 Dataset
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


    //创建Dataset
    val scores: Dataset[String] = spark.read.textFile("hdfs://hadoop01:8020/score")


    val scoreDataFrame = scores.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val studentId = fields(1).toLong
      val course = fields(2)
      val score = fields(3).toInt
      (id, studentId, course, score)

    }).toDF("id", "studentId", "course", "score")

    scoreDataFrame.createTempView("v_score")


    val stuScoreDataFrame = spark.sql("select  v_score.id,v_student.name,v_score.course,v_score.score from v_score left join v_student on v_student.id=v_score.studentId ")

    stuScoreDataFrame.show()

    //TODO：每位同学的平均分


    //TODO：每位同学的最高分

    spark.stop()


  }
}
