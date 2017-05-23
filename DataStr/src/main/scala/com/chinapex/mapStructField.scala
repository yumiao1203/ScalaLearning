package com.chinapex
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-18.
  */

trait MapStructHelper {
//  def mapStructField(colName: String): StructField = {
//    if (colName == "用户id") {
//      StructField(colName, IntegerType, true)
//    } else if (colName == "时间戳"|colName == "放款时间") {
//      StructField(colName, LongType, true)
//    } else if (colName == "交易金额" || colName == "上期账单金额" || colName == "上期还款金额"
//      || colName == "信用卡余额" || colName == "本期账单余额" || colName == "本期账单最低还款额"
//      || colName == "本期账单金额" || colName == "调整金额" || colName == "循环利息"
//      || colName == "可用余额" || colName == "预借现金额度") {
//      StructField(colName, DoubleType, true)
//    } else if (colName == "工资收入标记" || colName == "gender" || colName == "交易类型") {
//      StructField(colName, BooleanType, true)
//    } else {
//      StructField(colName, StringType, true)
//    }
//  }
  def mapStructField(colName: String): StructField = {
    StructField(colName, StringType, true)
  }
}
