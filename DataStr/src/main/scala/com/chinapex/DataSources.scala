package com.chinapex

/**
  * Created by josh on 17-5-17.
  */


import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object DataSources extends App{

  // Load a text file and interpret each line as a java.lang.String
  // read txt file ,create an RDD
  val sc = new SparkContext("local[3]","APPName")
  //  val filePath = "/home/josh/个人征信/train/user_info_train.txt"  //Current fold file
  //  val user = sc.textFile(filePath)

//  //read csv

  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("Spark Reader")
    .getOrCreate
//
//  val filePath = "./DataStr/src/main/resources/titanic/train.csv"
//  val df = spark.read
//    .format("com.databricks.spark.csv")
//    .option("header", "true") //reading the headers
//    .option("inferSchema", "true")
//    .load(filePath) //.csv("csv/file/path") //spark 2.0 api
//
//  df.show(5)

  //  read txt, create an RDD
  val peopleRDD = spark.sparkContext.textFile("./DataStr/src/main/resources/people.txt")
  // The schema is encoded in a string
  val schemaString = "name age"
  // Generate the schema based on the string of schema
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)
  val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
  // Apply the schema to the RDD
  val peopleDF = spark.createDataFrame(rowRDD, schema)
  peopleDF.show()

  //  read json, create a DataFrame
 // val peopleDF = spark.read.json("./DataStr/src/main/resources/people.json")
  //peopleDF.show()
  //peopleDF.printSchema()


//  val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
//  usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")

}

