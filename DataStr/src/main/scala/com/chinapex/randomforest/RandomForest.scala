package com.chinapex.randomforest

/**
  * Created by josh on 17-5-23.
  */
import com.chinapex.process.Test
import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


// Load and parse the data file.
object RandomForestTest extends App {
  val sc = new SparkContext("local[3]","AppName")
  val spark = SparkSession.builder
    .master("local")
    .appName("Spark read csv")
    .getOrCreate
  val colName = Test.DF2.columns
  val filePath = "user_loan_overdue.csv" //Current fold file
  val featureList = List("loan_time","gender","occupation","education","marriage")
  val targetList = List("tag")
  val dfColumn = List.concat(featureList, targetList)
  //读取csv的另一种方法as dataframe
  val DF = spark.read
    .format("csv")
    .option("header", false)
    .option("inferSchema", "true")
    .load(filePath).toDF(colName: _*)
  // .load(filePath).toDF("user_id","tag","loan_time","gender","occupation","education","marriage","household_type")
 // val data = MLUtils.loadLibSVMFile(sc, "user_loan_overdue.csv")
  val tmpDF = DF.select(dfColumn.map(col): _*).toDF(dfColumn:_*)


  val featuresColName = "features"
  val labelColName = "label"

  val test11 = data.select("labelColName").rdd.map(r => r(0)).collect()
  test11

  val data = tmpDF.withColumnRenamed(targetList.head, labelColName)
  val assembler = new VectorAssembler().setInputCols(featureList.toArray).setOutputCol(featuresColName)

  val vecDF: DataFrame = assembler.transform(data)

  vecDF.show(2)
  val Array(trainingDF, testDF) = vecDF.randomSplit(Array(0.7, 0.3), seed = 123456)
  // Train a RandomForest model.
  val rf = new RandomForestClassifier()
    .setLabelCol(labelColName)
    .setFeaturesCol(featuresColName)

  val paramGrid = new ParamGridBuilder()
    .addGrid(rf.numTrees, Array(1, 5, 20))
    .addGrid(rf.subsamplingRate, Array(0.1, 0.5, 1))
    .build()

  val evaluatorName =
    new MulticlassClassificationEvaluator()
      .setLabelCol(labelColName)
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

  val trainValidationSplit = new TrainValidationSplit()
    .setEstimator(rf)
    .setEvaluator(evaluatorName)
    .setEstimatorParamMaps(paramGrid)
    // 80% of the data will be used for training and the remaining 20% for validation.
    .setTrainRatio(0.8)

  val model = trainValidationSplit.fit(trainingDF)
  val numTrees = model.bestModel.extractParamMap()(model.bestModel.getParam("numTrees"))
  val subsamplingRate = model.bestModel.extractParamMap()(model.bestModel.getParam("subsamplingRate"))

  rf
    .setNumTrees(numTrees.toString.toInt)
    .setSubsamplingRate(subsamplingRate.toString.toDouble)

  val rfModelTest = rf.fit(trainingDF)
  val testResult = rfModelTest.transform(testDF)

  val accuracy = model.getEvaluator.evaluate(testResult)

  println(s"print best model params ${model.bestModel.params}")
  println(s"print best model extractParamMap ${model.bestModel.extractParamMap()}")
  println(s"print evaluator ${accuracy}")

}
