package com.chinapex

/**
  * Created by josh on 17-5-18.
  */
import breeze.linalg.{DenseMatrix, DenseVector, SparseVector, max}
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.sql.Row
object Launcher extends App{

    val sc = new SparkContext("local[3]","AppName")
    val filePath = "/home/josh/Downloads/input/train.csv"
    val spark = SparkSession.builder
      .master("local")
      .appName("Spark CSV Reader")
      .getOrCreate
    val df = spark.read
      .format("csv")
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

    df.sort("Age","Name").show(10)

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
    val dft = dfNew1.collect()
    val df22 = dfNew1.col(dfNew.columns(1))


    //df.describe("Age").show()
    //val df2 = df.withColumnRenamed("Sex", "sex")
    //df2.show(5)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._ // for `toDF` and $""
    import org.apache.spark.sql.functions._ // for `when`

    val dataf = sc.parallelize(Seq((4, "blah", 2), (2, "", 3), (56, "foo", 3), (100, null, 5)))
      .toDF("A", "B", "C")

    val newDf = dataf.withColumn("D", when($"B".isNull or $"B" === "", 0).otherwise(1))
    newDf.show()
    //新列名取"sex"时会替代原来的"Sex"
   // val newData = df.withColumn("sex",when($"Sex" === "male",1).when($"Sex" === "female",0))
    val newData = df.withColumn("gender",when($"Sex" === "male",1).when($"Sex" === "female",0))
    newData.show(2)
    val clearData = newData.filter("gender is not null and Age is not null and Cabin is not null")select (
      "Survived","Age","gender","Cabin")
    val clearNum = clearData.count()
    println(clearNum)
    clearData.printSchema()

    //create a matrix
    val values = Array(0.0, 1.0, 1.0, -1, 0,
      1, 1, 0, 0, -1,
      0, 0, 0, -1, 1,
      1, 1, 0 , 0, -1,
      -1, 0, 0, 0, 0)
    val matrix = Matrices.dense(5, 5, values)

    println(matrix)

    val gender1 = clearData.select("gender")
    gender1.show(6)


    //
    //  //SQL statements can be run by using the sql methods provided by Spark
    //  val teenagersDF = spark.sql("SELECT Name, Age FROM df1 WHERE Age BETWEEN 13 AND 19")
    //

    //
    //  // Create an RDD
    //  val peopleRDD = spark.sparkContext.textFile("/home/josh/Downloads/input/people.txt")
    //
    //  // The schema is encoded in a string
    //  val schemaString = "name age"
    //  // Generate the schema based on the string of schema
    //  val fields = schemaString.split(" ")
    //    .map(fieldName => StructField(fieldName, StringType, nullable = true))
    //  val schema = StructType(fields)
    //  // Convert records of the RDD (people) to Rows
    //  val rowRDD = peopleRDD
    //    .map(_.split(","))
    //    .map(attributes => Row(attributes(0), attributes(1).trim))
    //
    //  // Apply the schema to the RDD
    //  val peopleDF = spark.createDataFrame(rowRDD, schema)
    //  peopleDF.printSchema()
    //
    //  // Creates a temporary view using the DataFrame
    //  peopleDF.createOrReplaceTempView("people")
    //
    //  // SQL can be run over a temporary view created using DataFrames
    //  val results = spark.sql("SELECT name FROM people")
    //  results.printSchema()


    sc.stop()

}
