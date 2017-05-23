package com.chinapex

import org.apache.spark.sql.{Row, SparkSession}
/**
  * Created by josh on 17-5-19.
  */
class OverdueTest {}
object OverdueTest {
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark read csv")
    .getOrCreate
  val filePath = "/home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/个人征信/train/overdue_train.csv" //Current fold file
  //读取csv的另一种方法
  val overdueDF =spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath).toDF("user_id","tag")
}
