package com.chinapex

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-18.
  */
class LoanTest{}

object LoanTest extends MapStructHelper{
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("loan information")
    .getOrCreate
  val filePath = "/home/josh/Downloads/个人征信/train/loan_time_train.txt" //Current fold file
  val loanRDD = spark.sparkContext.textFile(filePath)
  val schemaString = "user_id,loan_time"
  //通过columnname转换StructField

  val fields = schemaString.split(",").map(mapStructField)
  val schema = StructType(fields)

  val rowRDD = loanRDD.map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3)))
  // Apply the schema to the RDD
  val loanDF = spark.createDataFrame(rowRDD, schema).toDF()
  loanDF.printSchema()
  //loanDF.show()

}
