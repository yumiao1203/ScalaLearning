package com.chinapex.process

import org.apache.spark.sql.SparkSession
import com.chinapex._

/**
  * Created by josh on 17-5-18.
  */
object Test extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local")
    .getOrCreate()
  def lossL2squaredGrad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    prediction - actual
  }
  def lossL1Grad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    // a subgradient of L1
    math.signum(prediction - actual)
  }
  def mixedLossGrad(i: Int, j: Int, prediction: Double, actual: Double): Double = {
    // weird loss function subgradient for demonstration
    if (i + j % 2 == 0) lossL1Grad(i, j, prediction, actual) else lossL2squaredGrad(i, j, prediction, actual)
  }
  val userDF = UserCSVTest.userDF
  val overdueDF = OverdueTest.overdueDF
  val loanDF = LoanCSVTest.loanDF
  val bankDF = BankCSVTest.bankDF
  val billDF = BillCSVTest.billDF
  val browseDF = BrowseCSVTest.browseDF
  val DF1 = loanDF.join(userDF, "user_id")
  val DF2 = overdueDF.join(DF1, "user_id")
  DF2.show(5)
  //按性别分组
  //DF2.groupBy("gender").count().show()
  DF2.groupBy("education").count().show()
  //DF2.groupBy("tag").count().show()
  val clearDF = DF2.filter("gender != 0").count()
  billDF.show(5)
//  val browse_miss = browseDF.filter("browse_time is null").count()
//  print(browse_miss)

  browseDF.createOrReplaceTempView("browse")
  spark.sql("SELECT user_id FROM browse").show()
  // Register the function to access it

  spark.udf.register("myAverage", Average)

  DF2.createOrReplaceTempView("user_table")

  val result = spark.sql("SELECT myAverage(tag) as aver_tag FROM user_table")
  result.show()
//  DF2.select("user_id", "tag","education").write.format("csv").save("user_tag_edu.csv")
  val groupByTagEdu =spark.sql("SELECT * FROM user_table GROUP By tag")


  val testdata =spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load("user_tag_edu.csv").toDF("user_id","tag","education")
  testdata.show()
}
