/**
  * Created by josh on 17-5-17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object DataStructure extends App{
  //DataFrame

  // Load a text file and interpret each line as a java.lang.String
  val sc = new SparkContext("local[3]","AppName")
  val path = "/home/josh/个人征信/train/user_info_train.txt"  //Current fold file
  val rdd1 = sc.textFile(path,2)




}
