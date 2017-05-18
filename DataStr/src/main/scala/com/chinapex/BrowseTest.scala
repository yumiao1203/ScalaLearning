package com.chinapex

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-18.
  */
object BrowseTest extends App{
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("browse information")
    .getOrCreate
  val filePath = "/home/josh/Downloads/个人征信/train/browse_history_train.txt" //Current fold file
  val browseRDD = spark.sparkContext.textFile(filePath)
  val schemaString = "用户id,时间戳,浏览行为数据,浏览子行为编号"
  //通过columnname转换StructField
  def mapStructField(colName: String): StructField = {
    if (colName == "用户id") {
      StructField(colName, IntegerType, true)
    } else if (colName == "时间戳") {
      StructField(colName, LongType, true)
    } else if (colName == "交易金额" || colName == "上期账单金额" || colName == "上期还款金额"
      || colName == "信用卡余额" || colName == "本期账单余额" || colName == "本期账单最低还款额"
      || colName == "本期账单金额" || colName == "调整金额" || colName == "循环利息"
      || colName == "可用余额" || colName == "预借现金额度") {
      StructField(colName, DoubleType, true)
    } else if (colName == "工资收入标记" || colName == "性别" || colName == "交易类型") {
      StructField(colName, BooleanType, true)
    } else {
      StructField(colName, StringType, true)
    }
  }
  val fields = schemaString.split(",").map(mapStructField)
  val schema = StructType(fields)

  val rowRDD = browseRDD.map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3)))
  // Apply the schema to the RDD
  val browseDF = spark.createDataFrame(rowRDD, schema)
  browseDF.printSchema()
  //bankDF.show
}
