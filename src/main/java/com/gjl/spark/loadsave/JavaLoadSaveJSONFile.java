package com.gjl.spark.loadsave;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Iterator;

public class JavaLoadSaveJSONFile {

    public static class Person implements java.io.Serializable {
        public String name;
        public boolean lovesPandas;
    }

    public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
        public Iterator<Person> call(Iterator<String> lines) throws Exception {
            ArrayList<Person> people = new ArrayList<Person>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    people.add(mapper.readValue(line, Person.class));
                } catch (Exception e) {
                    // Skip invalid input
                }
            }
            return people.iterator();
        }
    }

    public static class LikesPandas implements Function<Person, Boolean> {
        public Boolean call(Person person) {
            return person.lovesPandas;
        }
    }

    public static class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
        public Iterator<String> call(Iterator<Person> people) throws Exception {
            ArrayList<String> text = new ArrayList<String>();
            ObjectMapper mapper = new ObjectMapper();
            while (people.hasNext()) {
                Person person = people.next();
                text.add(mapper.writeValueAsString(person));
            }
            return text.iterator();
        }
    }

    public static void main(String[] args) {
        String master = "local";
        if (args.length > 0) {
            master = args[0];
        }

        JavaSparkContext sc = new JavaSparkContext(master, "JavaLoadSaveJSONFile",
                System.getenv("SPARK_HOME"), System.getenv("JARS"));
        JavaRDD<String> input = sc.textFile("src/main/resources/file.json");
        JavaRDD<Person> result = input.mapPartitions(new ParseJson()).filter(new LikesPandas());
        System.out.println(result.collect());
        JavaRDD<String> formatted = result.mapPartitions(new WriteJson());
        formatted.saveAsTextFile("/tmp/ResultRDD.json");

    }
}
