package com.gjl.spark.loadsave;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.StringReader;
import java.util.Iterator;

public class JavaLoadSaveCSVFile {
    public static class ParseLine implements FlatMapFunction<Tuple2<String, String>, String[]> {
        public Iterator<String[]> call(Tuple2<String, String> file) throws Exception {
            CSVReader reader = new CSVReader(new StringReader(file._2()));
            return reader.readAll().iterator();
        }
    }

    public static void main(String[] args) {
        String master = "local";
        if (args.length > 0) {
            master = args[0];
        }

        JavaSparkContext sc = new JavaSparkContext(master, "JavaLoadSaveJSONFile",
                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaPairRDD<String, String> csvData = sc.wholeTextFiles("src/main/resources/favourite_animals.csv");
        JavaRDD<String[]> keyedRDD = csvData.flatMap(new ParseLine());
        System.out.println(keyedRDD.collect());
        JavaRDD<String> result = keyedRDD.filter((Function<String[], Boolean>) input -> input[0].equals("holden"))
                .map((Function<String[], String>) input -> input[1]);
        result.saveAsTextFile("/tmp/ResultRDD.csv");
    }
}
