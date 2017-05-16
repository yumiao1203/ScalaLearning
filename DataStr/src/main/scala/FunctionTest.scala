/**
  * Created by josh on 17-5-16.
  */
object FunctionTest {
  def main(args: Array[String]) {
    val n = timesTwo(2) + addOne(1)
    println("Hello,\tWorld!\n" + n + multiplier(3))
    time()
  }

  def timesTwo(i: Int): Int = {
    i * 2
  }

  def addOne(i: Int): Int = {
    i + 1
  }

  def time() = {
    println("时间为",System.nanoTime,"纳秒")
  }
  var factor = 3
  //multiplier为闭包，形式参数为i，此外还依赖函数外的变量
  val multiplier = (i:Int) => i * factor
}

