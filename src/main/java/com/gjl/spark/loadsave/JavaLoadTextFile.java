package com.gjl.spark.loadsave;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class JavaLoadTextFile {
    public static void main(String[] args) {
        String master = "local";
        if (args.length > 0) {
            master = args[0];
        }

        JavaSparkContext sc = new JavaSparkContext(master, "JavaLoadTextFile",
                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile("src/main/resources/README.md");
        System.out.println(input.collect());
        sc.close();
    }
}
