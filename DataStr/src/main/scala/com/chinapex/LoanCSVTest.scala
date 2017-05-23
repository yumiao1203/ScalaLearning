package com.chinapex

import org.apache.spark.sql.SparkSession

/**
  * Created by josh on 17-5-22.
  */
//class LoanCSVTest {}
object LoanCSVTest{
  val filePath = "/home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/个人征信/train/loan_time_train.csv"
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark CSV read")
    .getOrCreate
  val loanDF = spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath).toDF("user_id","loan_time")
  //loanDF.show(5)
 // loanDF.printSchema()
}
