package com.chinapex

import org.apache.spark.sql.SparkSession

/**
  * Created by josh on 17-5-23.
  */
object BillCSVTest {
  val filePath = "/home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/个人征信/train/bill_detail_train.txt"
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark CSV read")
    .getOrCreate
  val billDF = spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath).toDF("user_id","bill_time","bank_id","上期账单金额",
    "上期还款金额","信用卡余额","本期账单余额","本期账单最低还款额","消费笔数",
    "本期账单金额","调整金额","循环利息","可用余额","预借现金额度","还款状态")
}
