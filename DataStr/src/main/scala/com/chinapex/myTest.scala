package com.chinapex
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame,Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/**
  * Created by josh on 17-5-22.
  */
object myTest {
  val sc = new SparkContext("local[3]","AppName")
//SparkSession 类是到 Spark SQL 所有功能的入口点，只需调用 SparkSession.builder() 即可创建
  val spark = SparkSession
    .builder()
    .appName("Spark SQL Example")
    .config("spark.some.config.option", "some-value")
    .getOrCreate()
  // 包含隐式转换（比如讲 RDDs 转成 DataFrames）API
  import spark.implicits._
  //使用 SparkSession，可以从已经在的 RDD、Hive 表以及 Spark 支持的数据格式创建。
  // 下面这个例子就是读取一个 Json 文件来创建一个 DataFrames
  val df = spark.read.json("/home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/people.json")
  df.show()
  df.select($"name", $"age" + 1).show()
  df.filter($"age" > 21).show()
  // Count people by age
  df.groupBy("age").count().show()
  val df1 = spark.read.json("/home/josh/IdeaProjects/ScalaLearning/DataStr/src/main/resources/people.json")

  df.withColumn("data",df.col("name")).printSchema()


  def main(args: Array[String]): Unit = {

  }
}
