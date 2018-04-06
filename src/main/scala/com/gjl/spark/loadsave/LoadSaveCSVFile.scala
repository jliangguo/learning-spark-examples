package com.gjl.spark.loadsave

import java.io.{StringReader, StringWriter}

import scala.collection.JavaConversions._

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import org.apache.spark.SparkContext

object LoadSaveCSVFile {
  case class Person(name: String, favouriteAnimal: String)

  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadTextFile", System.getenv("SPARK_HOME"))
    val input = sc.textFile("src/main/resources/favourite_animals.csv")
    val result = input.map { line =>
      val reader = new CSVReader(new StringReader(line))
      reader.readNext()
    }
    val people = result.map(x => Person(x(0), x(1)))
    val pandaLovers = people.filter(person => person.favouriteAnimal == "panda")
    pandaLovers.map(person => List(person.name, person.favouriteAnimal).toArray).mapPartitions{ people =>
      val stringWriter = new StringWriter()
      val csvWriter = new CSVWriter(stringWriter)
      csvWriter.writeAll(people.toList)
      Iterator(stringWriter.toString)
    }.saveAsTextFile("/tmp/ResultRDD.csv")

  }
}
