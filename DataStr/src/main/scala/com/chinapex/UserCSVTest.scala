package com.chinapex

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._ //udf to change schema

class  UserCSVTest{}
/**
  * Created by mia on 17-5-18.
  */
object UserCSVTest{
 // val sc = new SparkContext("local[3]","AppName")
//  val source = scala.io.Source.fromFile("/home/josh/Downloads/个人征信/train/user_info_train.txt")
//  val lines = try source.mkString finally source.close()
  val filePath = "file:///home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/个人征信/train/user_info_train.txt" //Current fold file
  val spark = SparkSession.builder
    .master("local")
    .appName("user CSV reader")
    .getOrCreate
  val userDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "false")
    //.load(filePath).toDF("用户id", "性别", "职业", "教育程度", "婚姻状态", "户口类型")
    .load(filePath).toDF("user_id", "gender", "occupation", "education", "marriage","household_type")
  userDF.show(5)
  userDF.printSchema()
  //改变表结构
//  val toInt = udf[Int, String](_.toInt)
//  val toDouble = udf[Double, String]( _.toDouble)
//  val featureDf = userDF
//    .withColumn("user_id", toInt(userDF("User_id")))
//    .withColumn("gender", toInt(userDF("Gender")))
//    .withColumn("occupation", toInt(userDF("Occupation")))
//    .withColumn("education", toInt(userDF("Education")))
//    .withColumn("marriage", toInt(userDF("Marriage")))
//    .withColumn("household_type", toInt(userDF("Household_type")))
//  featureDf.printSchema()
//  featureDf.show(5)
}
