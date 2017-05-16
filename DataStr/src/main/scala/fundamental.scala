/**
  * Created by josh on 17-5-16.
  */
/**
  * Scala 基本语法需要注意以下几点
  * 区分大小写 -  Scala是大小写敏感的，这意味着标识Hello 和 hello在Scala中会有不同的含义。
类名 - 对于所有的类名的第一个字母要大写。

如果需要使用几个单词来构成一个类的名称，每个单词的第一个字母要大写。

示例：class MyFirstScalaClass
方法名称 - 所有的方法名称的第一个字母用小写。

如果若干单词被用于构成方法的名称，则每个单词的第一个字母应大写。

程序文件名 - 程序文件的名称应该与对象名称完全匹配。

保存文件时，应该保存它使用的对象名称（记住Scala是区分大小写），并追加".scala"为文件扩展名。 （如果文件名和对象名称不匹配，程序将无法编译）。

示例: 假设"HelloWorld"是对象的名称。那么该文件应保存为'HelloWorld.scala"
def main(args: Array[String]) - Scala程序从main()方法开始处理，这是每一个Scala程序的强制程序入口部分。
  */

object HelloWorld {
  def main(args: Array[String]) {
    val n = timesTwo(2)+addOne(1)
    println("Hello,\tWorld!\n" + n)
  }
  def timesTwo(i : Int): Int = {
    i*2
  }
  def addOne(i : Int): Int = {
    i+1
  }
}





