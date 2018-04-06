package com.gjl.spark.loadsave

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.SparkContext

object LoadSaveSequenceFile {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadSaveSequenceFile", System.getenv("SPARK_HOME"))
    val data = sc.parallelize(List(("Panda", 3), ("Kay", 6), ("Snail", 2)))
    data.saveAsSequenceFile("src/main/resources/seq")

    val result = sc.sequenceFile("src/main/resources/seq", classOf[Text], classOf[IntWritable]).map{case (x, y) =>
      (x.toString, y.get())}
    println(result.collect().toList)
  }
}
