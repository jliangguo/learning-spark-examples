package com.gjl.spark.loaddata

import org.apache.spark.SparkContext

/** 使用textFile方法从文本文件加载数据 */
object LoadTextFile {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadTextFile", System.getenv("SPARK_HOME"))
    val input = sc.textFile("src/main/resources/README.md")
    println(input.collect().mkString(","))
    sc.stop()
  }
}
