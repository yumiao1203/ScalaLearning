package com.chinapex

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-17.
  */
object BankTest extends App with MapStructHelper{
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("bank information")
      .getOrCreate
    val filePath = "/home/josh/Downloads/个人征信/train/bank_detail_train.txt" //Current fold file
    val bankRDD = spark.sparkContext.textFile(filePath)
    val schemaString = "用户id 时间戳 交易类型 交易金额 工资收入标记"

    val fields = schemaString.split(" ").map(mapStructField)
    val schema = StructType(fields)

    val rowRDD = bankRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3),
        attributes(4)))
    // Apply the schema to the RDD
    val bankDF = spark.createDataFrame(rowRDD, schema)
    bankDF.printSchema()
  var dfNew :DataFrame = bankDF
  for( i <- 0 to 4){
   // val newName = Array("user_id","time","transaction_type","transaction_money","income_tag")
   val newName = Array("user","time","transaction","transaction","income")
    dfNew = dfNew.withColumnRenamed(dfNew.columns(i),newName(i))
  }
    //dfNew.show()
  dfNew.printSchema()
  //dfNew.show(5)

}
