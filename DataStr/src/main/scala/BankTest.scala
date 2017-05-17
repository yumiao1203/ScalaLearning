import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-17.
  */
object BankTest {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("bank information")
      .getOrCreate
    val filePath = "/home/josh/Downloads/个人征信/train/bank_detail.txt" //Current fold file
    val bankRDD = spark.sparkContext.textFile(filePath)
    val schemaString = "用户id 时间戳 交易类型 交易金额 工资收入标记"

//    def getStrucType(colname:String): StructType ={
//      if (colname == '用户id'|| colname == '时间戳')
//
//    }
    //    val aStruct = new StructType(Array(
    //      StructField("用户id",StringType,nullable = true),
    //      StructField("时间戳",StringType,nullable = true),
    //      StructField("交易类型",BooleanType,nullable = true),
    //      StructField("交易金额",DoubleType,nullable = true),
    //      StructField("工资收入标记",BooleanType,nullable = true)
    //    ))
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

//    val aStruct = new StructType(Array(
//      StructField("用户id",StringType,nullable = true),
//      StructField("时间戳",StringType,nullable = true),
//      StructField("交易类型",BooleanType,nullable = true),
//      StructField("交易金额",DoubleType,nullable = true),
//      StructField("工资收入标记",BooleanType,nullable = true)
//    ))


    val rowRDD = bankRDD.map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3),
        attributes(4)))
    // Apply the schema to the RDD
    val bankDF = spark.createDataFrame(rowRDD, schema)
    bankDF.printSchema()
    //bankDF.show

  }
}
