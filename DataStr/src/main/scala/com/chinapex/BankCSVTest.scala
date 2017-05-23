package com.chinapex

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

/**
  * Created by josh on 17-5-18.
  */
object BankCSVTest extends App {
  // val sc = new SparkContext("local[3]","AppName")
  //  val source = scala.io.Source.fromFile("/home/josh/Downloads/个人征信/train/user_info_train.txt")
  //  val lines = try source.mkString finally source.close()
  val filePath = "file:///home/josh/Downloads/个人征信/train/bank_detail_train.txt" //Current fold file
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark read csv1")
    .getOrCreate
  val bankDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "false")
    .load(filePath).toDF("用户id", "时间戳", "交易类型", "交易金额", "工资收入标记")
  //userDF.show()
  bankDF.printSchema()
  //改变表结构
  val toInt = udf[Int, String](_.toInt)
  val toDouble = udf[Double, String](_.toDouble)
  val toBoolean = udf[Boolean, String](_.toBoolean)
  val featureDf = bankDF
    .withColumn("用户id", toInt(bankDF("用户id")))
    .withColumn("时间戳", toInt(bankDF("时间戳")))
    .withColumn("交易类型", toBoolean(bankDF("交易类型")))
    .withColumn("交易金额", toDouble(bankDF("交易金额")))
    .withColumn("工资收入标记", toBoolean(bankDF("工资收入标记")))
  //featureDf.printSchema()
  bankDF.printSchema()
  bankDF.show(5)
}

