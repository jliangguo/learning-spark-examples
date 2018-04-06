package com.gjl.spark.loadsave

import org.apache.spark.SparkContext

object LoadSaveObjectFile {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadSaveSequenceFile", System.getenv("SPARK_HOME"))
    val data = sc.parallelize(1 to 100, 4)
    data.saveAsObjectFile("src/main/resources/objectFile")

    val input = sc.objectFile[Int]("src/main/resources/objectFile")
    println(input.collect().mkString(","))
  }
}
