package com.gjl.spark.loadsave

import com.gjl.spark.proto.Places
import com.twitter.elephantbird.mapreduce.io.ProtobufWritable
import com.twitter.elephantbird.mapreduce.output.LzoProtobufBlockOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext

object LoadSaveProtobuf {
  def main(args: Array[String]): Unit = {
    val master = args.length match {
      case x: Int if x > 0 => args(0)
      case _ => "local"
    }
    val sc = new SparkContext(master, "LoadSaveProtobuf", System.getenv("SPARK_HOME"))
    val job = new Job()
    val conf = job.getConfiguration
    LzoProtobufBlockOutputFormat.setClassConf(classOf[Places.Venue], conf)
    val dnaLounge = Places.Venue.newBuilder()
    dnaLounge.setId(1)
    dnaLounge.setName("DNA Lounge")
    dnaLounge.setType(Places.Venue.VenueType.CLUB)
    val data = sc.parallelize(List(dnaLounge.build()))
    val outputData = data.map { pb =>
      val protoWritable = ProtobufWritable.newInstance(classOf[Places.Venue])
      protoWritable.set(pb)
      (null, protoWritable)
    }
    outputData.saveAsNewAPIHadoopFile("src/main/resources/proto", classOf[Text], classOf[ProtobufWritable[Places.Venue]],
      classOf[LzoProtobufBlockOutputFormat[ProtobufWritable[Places.Venue]]], conf)
  }
}
