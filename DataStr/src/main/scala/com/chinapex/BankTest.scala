package com.chinapex

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-17.
  */
object BankTest {
  def main(args: Array[String]) {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("bank information")
      .getOrCreate
    val filePath = "/home/josh/Downloads/个人征信/train/bank_detail.txt" //Current fold file
    val bankRDD = spark.sparkContext.textFile(filePath)
    val schemaString = "用户id 时间戳 交易类型 交易金额 工资收入标记"
//通过columnname转换StructField
    def mapStructField(colName:String): StructField ={
      if(colName == "用户id"){
        StructField(colName, IntegerType, true)
      }else if(colName == "时间戳"){
        StructField(colName, LongType, true)
      }else if(colName == "交易金额") {
        StructField(colName, DoubleType, true)
      }else if(colName == "工资收入标记"||colName == "性别"||colName == "交易类型"){
        StructField(colName, BooleanType, true)
      }else{
        StructField(colName, StringType, true)
      }
    }

    val fields = schemaString.split(" ").map(mapStructField)
    val schema = StructType(fields)

    val rowRDD = bankRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3),
        attributes(4)))
    // Apply the schema to the RDD
    val bankDF = spark.createDataFrame(rowRDD, schema)
    bankDF.printSchema()
    //bankDF.show

  }
}
