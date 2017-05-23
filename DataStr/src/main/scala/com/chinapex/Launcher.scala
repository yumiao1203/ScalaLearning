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


    //  val dfColumnAss = List("Age", "gender")
    //
    //  val assembler = new VectorAssembler().setInputCols(dfColumnAss.toArray).setOutputCol("features")

    //  val vecDF: DataFrame = assembler.transform(clearData)
    //  val labelColumn = "Survived"
    //
    //  val lr = new LinearRegression()
    //    .setMaxIter(10)
    //    .setRegParam(0.3)
    //    .setElasticNetParam(0.8)
    //
    //  val lr1 = lr.setFeaturesCol("features").setLabelCol(labelColumn)
    //    .setFitIntercept(true).setStandardization(false)
    //
    //  val lrModel = lr.fit(vecDF)
    //  println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")
    //  val trainingSummary = lrModel.summary
    //  println(s"numIterations: ${trainingSummary.totalIterations}")
    //  println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //  trainingSummary.residuals.show()
    //  println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //  println(s"r2: ${trainingSummary.r2}")

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

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    // The columns of a row in the result can be accessed by field index or by field name
    // results.map(attributes => "Name: " + attributes(0)).show()

    //  df.show(10)
    //  val rowNumRaw = df.count()
    //  println(rowNumRaw)
    //  df.printSchema()
    //  val data = df.filter("Cabin is not null ")
    //  val data1 = data.filter("Age is not null")
    //  val rowNum1 = data1.count()
    //  println(rowNum1)
    //
    //  val rowNum = data.count()
    //  println(rowNum)
    //  df.show(7)
    //  data.show(7)
    //  data.printSchema()
    //  data1.createOrReplaceTempView("people")
    //  data1.createGlobalTempView("people")
    //  spark.sql("SELECT Age AND Sex FROM global_temp.people").show()
    //
    //  val documentDF = spark.createDataFrame(Seq(
    //    "Hi I heard about Spark".split(" "),
    //    "I wish Java could use case classes".split(" "),
    //    "Logistic regression models are neat".split(" ")
    //  ).map(Tuple1.apply)).toDF("text")
    //
    //
    //  // Learn a mapping from words to Vectors.
    //  val word2Vec = new Word2Vec()
    //    .setInputCol("text")
    //    .setOutputCol("result")
    //    .setVectorSize(3)
    //    .setMinCount(0)
    //  val model = word2Vec.fit(documentDF)
    //
    //  val result = model.transform(documentDF)
    //  result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
    //    println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

    // Prepare training data from a list of (label, features) tuples.
//    val training = spark.createDataFrame(Seq(
//      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
//      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
//      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
//      (1.0, Vectors.dense(0.0, 1.2, -0.5))
//    )).toDF("label", "features")
//
//    // Create a LogisticRegression instance. This instance is an Estimator.
//    val lr = new LogisticRegression()
//    // Print out the parameters, documentation, and any default values.
//    println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
//
//
//
//    // We may set parameters using setter methods.
//    lr.setMaxIter(10)
//      .setRegParam(0.01)
//
//    // Learn a LogisticRegression model. This uses the parameters stored in lr.
//    val model1 = lr.fit(training)
//    // Since model1 is a Model (i.e., a Transformer produced by an Estimator),
//    // we can view the parameters it used during fit().
//    // This prints the parameter (name: value) pairs, where names are unique IDs for this
//    // LogisticRegression instance.
//    println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)
//
//    // We may alternatively specify parameters using a ParamMap,
//    // which supports several methods for specifying parameters.
//    val paramMap = ParamMap(lr.maxIter -> 20)
//      .put(lr.maxIter, 30)  // Specify 1 Param. This overwrites the original maxIter.
//      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)  // Specify multiple Params.
//
//    // One can also combine ParamMaps.
//    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")  // Change output column name.
//    val paramMapCombined = paramMap ++ paramMap2
//
//    // Now learn a new model using the paramMapCombined parameters.
//    // paramMapCombined overrides all parameters set earlier via lr.set* methods.
//    val model2 = lr.fit(training, paramMapCombined)
//    println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)
//
//    // Prepare test data.
//    val test = spark.createDataFrame(Seq(
//      (1.0, Vectors.dense(1.0, 2.0, 3.0)),
//      (0.0, Vectors .dense(2.0 ,1.0, -1.0))
//    )).toDF("label","features")
//
//    // Make predictions on test data using the Transformer.transform() method.
//    // LogisticRegression.transform will only use the 'features' column.
//    // Note that model2.transform() outputs a 'myProbability' column instead of the usual
//    // 'probability' column since we renamed the lr.probabilityCol parameter previously.
//    model2.transform(test)
//      .select("features", "label", "myProbability", "prediction")
//      .collect()
//      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
//        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
//      }
//    val VectorTranspose = DenseVector(1.0, 2.0, 3.0, 4.0)
//    print(VectorTranspose)

    sc.stop()

}
