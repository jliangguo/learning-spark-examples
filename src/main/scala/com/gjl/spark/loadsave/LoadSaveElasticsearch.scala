package com.gjl.spark.loadsave

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.SparkContext
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsInputFormat

object LoadSaveElasticsearch {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "BasicQueryHBase", System.getenv("SPARK_HOME"))
    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.set("mapred.output.format.class", "org.elasticsearch.hadoop.mr.EsoutputFormat")
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, "twitter/tweets")
    jobConf.set(ConfigurationOptions.ES_NODES, "localhost")
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    val jobConf2 = new JobConf(sc.hadoopConfiguration)
    jobConf2.set(ConfigurationOptions.ES_RESOURCE_READ, args(1))
    jobConf2.set(ConfigurationOptions.ES_NODES, args(2))
    val currentTweets = sc.hadoopRDD(jobConf2, classOf[EsInputFormat[Object, MapWritable]],
      classOf[Object], classOf[MapWritable])
    // Convert the MapWritable[Text, Text] tp Map[String, String]
    val tweets = currentTweets.map {case (key, value) => mapWritableToInput(value)}
    println(tweets)
  }

  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    import scala.collection.JavaConversions._
    in.map {case (k, v) => (k.toString, v.toString)}.toMap
  }
}
