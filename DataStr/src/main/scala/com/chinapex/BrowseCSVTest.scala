package com.chinapex

import org.apache.spark.sql.SparkSession

/**
  * Created by josh on 17-5-23.
  */
object BrowseCSVTest {
  val filePath = "/home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/个人征信/train/browse_history_train.txt"
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark CSV read")
    .getOrCreate
  val browseDF = spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath).toDF("user_id","browse_time","browse_act_data","browse_sub_number")

}
