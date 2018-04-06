package com.gjl.spark.loadsave

import org.apache.spark.SparkContext
import play.api.libs.json._

object LoadSaveJSONFile {
  implicit val personReads: Format[Person] = Json.format[Person]

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadTextFile", System.getenv("SPARK_HOME"))
    val input = sc.textFile("src/main/resources/file.json")
    val parsed = input.map(Json.parse) // one JSON record per row
    val result = parsed.flatMap(record => personReads.reads(record).asOpt)
    println(result.collect().mkString(","))
    result.filter(_.lovesPandas).map(Json.toJson(_)).saveAsTextFile("/tmp/ResultRDD.json")
  }
}

case class Person(name: String, lovesPandas: Boolean) // Must be a top-level class

