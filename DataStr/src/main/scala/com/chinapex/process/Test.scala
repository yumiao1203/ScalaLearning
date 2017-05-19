package com.chinapex.process


import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by josh on 17-5-18.
  */
object Test extends App {
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark read csv")
    .getOrCreate
  val filePath = "/home/josh/Downloads/个人征信/train/bill_detail_train.txt" //Current fold file
  //读取csv的另一种方法
  val billDF =spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath).toDF("用户id","账单时间戳","银行id","上期账单金额","上期还款金额","信用卡余额","本期账单余额","本期账单最低还款额","消费笔数","本期账单金额","调整金额","循环利息","可用余额","预借现金额度","还款状态")
  billDF.printSchema()

  val filePath1 = "/home/josh/Downloads/个人征信/train/loan_time_train.txt"
  val loanDF =spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath1).toDF("用户id","放款时间")
  loanDF.printSchema()

  //merge two dataFrame
  //val df = billDF.join(loanDF,billDF.col("用户id") === loanDF.col("用户id")) //有两列用户id
  val df = billDF.join(loanDF, Seq("用户id")) //只有一列用户id
  df.printSchema()
  df.show(5)
  //将dataframe保存为csv格式
  df.coalesce(1).write
    .option("header", true)
    .option("quote", "\u0000") //magic is happening here
    .csv("tempfile1")
  val rowNum = billDF.count()
  billDF.count()
  loanDF.count()
  println(rowNum)
}
