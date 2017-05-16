import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
  * Created by Mia on 17-5-16.
  */
object Titanic extends App{
  val sc = new SparkContext("local[3]","AppName")
  val filePath = "/home/josh/Downloads/input/train.csv"
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark CSV Reader")
    .getOrCreate
  val df = spark.read
    .format("com.databricks.spark.csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filePath)
  df.show(4)
  //Age+1
  df.select(df.col("Name"),df.col("Age").plus(1)).show(4)
  // 找出 Age>23的行
  df.filter(df.col("Age").gt(23)).show(4)
  // 找出 Age <23的行
  df.filter(df.col("Age").lt(23)).show(4)

  df.sort("Age").show(10)

  // DataFrame newdf = df.select(df.col("*")).where(df.col("Age").leq(10))
  val newdf = df.select(df.col("*")).where(df.col("Age").isNotNull)
  val newdf1 = newdf.select(newdf.col("*")).where(newdf.col("Cabin").isNotNull)
  newdf1.show(4)
  //按性别分组
  newdf1.groupBy("Sex").count().show()
  //Register the DataFrame as a temporary view
  df.createOrReplaceTempView("df1")
  //将列名改为小写 如何获取列数
  var dfNew :DataFrame = df
  for( i <- 0 to 11){
    dfNew = dfNew.withColumnRenamed(dfNew.columns(i),dfNew.columns(i).toLowerCase)
  }
  dfNew.show(4)
  //将列名改为大写
  var dfNew1 :DataFrame = df
  for( i <- 0 to 11){
    dfNew1 = dfNew1.withColumnRenamed(dfNew1.columns(i),dfNew1.columns(i).toUpperCase)
  }
  dfNew1.show(4)
  dfNew1.collect()

  val sqlContext = new SQLContext(sc)
  import sqlContext.implicits._ // for `toDF` and $""
  import org.apache.spark.sql.functions._ // for `when`
  //新列名取"sex"时会替代原来的"Sex"
  //val newData = df.withColumn("sex",when($"Sex" === "male",1).when($"Sex" === "female",0))
  val newData = df.withColumn("gender",when($"Sex" === "male",1).when($"Sex" === "female",0))
  newData.show(2)
  val clearData = newData.filter("gender is not null and Age is not null and Cabin is not null")select (
    "Survived","Age","gender","Cabin")
  val clearNum = clearData.count()
  println(clearNum)
  clearData.printSchema()

}
