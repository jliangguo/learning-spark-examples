package com.gjl.spark.loadsave

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext

object BasicQueryHBase {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicQueryHBase", System.getenv("SPARK_HOME"))
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, "tablename")
    val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    println(rdd.collectAsMap())
  }
}
