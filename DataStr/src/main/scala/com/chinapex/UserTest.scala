package com.chinapex

import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-17.
  */
object UserTest {
  def main(args: Array[String]) {
    // val sc = new SparkContext("local[3]", "APPName")
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("user information")
      .getOrCreate
    val filePath = "/home/josh/Downloads/个人征信/train/user_info_train.txt" //Current fold file
    val userRDD = spark.sparkContext.textFile(filePath)
    val schemaString = "用户id 性别 职业 教育程度 婚姻状态 户口类型"

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
    val rowRDD = userRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3),
        attributes(4), attributes(5)))
    // Apply the schema to the RDD
    val userDF = spark.createDataFrame(rowRDD, schema)
    userDF.printSchema()
    //userDF.show(5)

  }

    //  val testRow = Row(1,2,3)
    //  println(testRow)
}
