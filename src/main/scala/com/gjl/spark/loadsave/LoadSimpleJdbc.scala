package com.gjl.spark.loadsave

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD

object LoadSimpleJdbc {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadTextFile", System.getenv("SPARK_HOME"))
    val data = new JdbcRDD(sc, createConnection, sql = "SELECT * FROM demo WHERE ? <= id AND id <= ?",
      lowerBound = 1, upperBound = 3, numPartitions = 2, mapRow = extractValues)
    println(data.collect().toList)
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver").newInstance()
    DriverManager.getConnection("jdbc:mysql://localhost:3306/mysql", "root", "123456") // 需替换为正确的用户名密码
  }

  def extractValues(r: ResultSet) = {
    (r.getInt(1), r.getString(2))
  }
}
