import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Created by josh on 17-5-17.
  */
object UserTest extends App{
  val sc = new SparkContext("local[3]", "APPName")
  val spark = org.apache.spark.sql.SparkSession.builder
    .master("local")
    .appName("user information")
    .getOrCreate
  val filePath = "/home/josh/Downloads/个人征信/train/user_info_train.txt" //Current fold file
  val userRDD = spark.sparkContext.textFile(filePath)
  val schemaString = "用户id 性别 职业 教育程度 婚姻状况 户口类型"
  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName, StringType, nullable = true))
  val schema = StructType(fields)
  val rowRDD1 = userRDD.map(_.split(","))
    .map(attributes => Row(attributes(0), attributes(1), attributes(2), attributes(3),
      attributes(4), attributes(5)))
  // Apply the schema to the RDD
  val userDF = spark.createDataFrame(rowRDD1, schema)
  userDF.show(5)

    //  val testRow = Row(1,2,3)
    //  println(testRow)
}
