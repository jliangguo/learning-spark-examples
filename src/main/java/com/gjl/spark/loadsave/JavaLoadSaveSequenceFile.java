package com.gjl.spark.loadsave;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JavaLoadSaveSequenceFile {

    public static class ConvertToWritableTypes implements PairFunction<Tuple2<String, Integer>, Text, IntWritable> {

        @Override
        public Tuple2<Text, IntWritable> call(Tuple2<String, Integer> record) throws Exception {
            return new Tuple2<>(new Text(record._1), new IntWritable(record._2));
        }
    }

    public static class ConvertToNativeTypes implements PairFunction<Tuple2<Text, IntWritable>, String, Integer> {

        @Override
        public Tuple2<String, Integer> call(Tuple2<Text, IntWritable> record) throws Exception {
            return new Tuple2<>(record._1.toString(), record._2.get());
        }
    }

    public static void main(String[] args) {
        String master = "local";
        if (args.length > 0) {
            master = args[0];
        }

        JavaSparkContext sc = new JavaSparkContext(master, "JavaLoadSaveJSONFile",
                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        List<Tuple2<String, Integer>> input = new ArrayList();
        input.add(new Tuple2("coffee", 1));
        input.add(new Tuple2("coffee", 2));
        input.add(new Tuple2("pandas", 3));
        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(input);
        JavaPairRDD<Text, IntWritable> result = rdd.mapToPair(new ConvertToWritableTypes());
        result.saveAsHadoopFile("src/main/resources/seq", Text.class, IntWritable.class, SequenceFileOutputFormat.class);

        JavaPairRDD<Text, IntWritable> inputRdd = sc.sequenceFile("src/main/resources/seq", Text.class, IntWritable.class);
        JavaPairRDD<String, Integer> resultRdd = inputRdd.mapToPair(new ConvertToNativeTypes());
        List<Tuple2<String, Integer>> resultList = resultRdd.collect();
        for (Tuple2<String, Integer> record : resultList) {
            System.out.println(record);
        }
    }
}
