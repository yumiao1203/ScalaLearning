package com.chinapex

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-18.
  */
object BillTest extends App with MapStructHelper{
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("bill information")
    .getOrCreate
  val filePath = "/home/josh/Downloads/个人征信/train/bill_detail_train.txt" //Current fold file
  val billRDD = spark.sparkContext.textFile(filePath)

  //val colNamePath = "/home/josh/Downloads/个人征信/train/bill_colname.txt"
  val schemaString = "用户id,账单时间戳,银行id,上期账单金额,上期还款金额,信用卡余额,本期账单余额,本期账单最低还款额,消费笔数,本期账单金额,调整金额,循环利息,可用余额,预借现金额度,还款状态"
  val fields = schemaString.split(",").map(mapStructField)
  val schema = StructType(fields)
  val rowRDD = billRDD.map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3), attributes(4),
      attributes(5), attributes(6), attributes(7), attributes(8), attributes(9),
      attributes(10), attributes(11), attributes(12), attributes(13), attributes(14)))
  // Apply the schema to the RDD
  val billDF = spark.createDataFrame(rowRDD, schema)
  billDF.printSchema()
  //  billDF.select(billDF.col("*")).where(billDF.col("账单时间戳").isNotNull).show()
  //  billDF.filter(billDF.col("用户id").gt(3323)).show(4)

}
