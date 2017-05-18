package com.chinapex

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by josh on 17-5-18.
  */
object UserCSVTest extends App{
 // val sc = new SparkContext("local[3]","AppName")
//  val source = scala.io.Source.fromFile("/home/josh/Downloads/个人征信/train/user_info_train.txt")
//  val lines = try source.mkString finally source.close()
  val filePath = "/home/josh/Downloads/个人征信/train/user_info_train.csv" //Current fold file
  val spark = SparkSession.builder
    .master("local")
    .appName("usercsvtest")
    .getOrCreate
  val userDF = spark.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filePath)
  //userDF.show()
  userDF.printSchema()

}
