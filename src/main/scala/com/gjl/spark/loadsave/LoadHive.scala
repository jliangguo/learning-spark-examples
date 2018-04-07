package com.gjl.spark.loadsave

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

object LoadHive {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("Usage: [sparkmaster] [tablename]")
      System.exit(1)
    }
    val master = args(0)
    val tableName = args(1)
    val sc = new SparkContext(master, "LoadHive", System.getenv("SPARK_HOME"))
    val hiveCtx = new HiveContext(sc) // 2.X different
    val input = hiveCtx.sql(s"SELECT name, age FROM $tableName")
    val firstRow = input.first()
    println(firstRow.getString(0))
  }
}
