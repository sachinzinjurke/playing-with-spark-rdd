package com.spark.tutorial.rdd;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkFlatMapExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<String> inputData=new ArrayList<>();
        inputData.add("Sachin is learning Spark");
        inputData.add("Spark RDD");
        inputData.add("RDD is not difficult");
        inputData.add("getting ready");


        SparkConf conf=new SparkConf().setAppName("Spark Hello World").setMaster("local[3]");
        JavaSparkContext context=new JavaSparkContext(conf);

        //We are using Iterables api from Guava to get the size of pair RDD
        //As GroupByKey gives <Key,Iterable> object and get size we have to use Guava library
        context.parallelize(inputData)
                .flatMap(value-> Arrays.asList(value.split(" ")).iterator())
                .filter(word->word.length() > 3)
                .collect()
                .forEach(System.out::println);

    context.close();

    }
}
