package com.spark.tutorial.rdd;

import com.google.common.collect.Iterables;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkPairRddGroupByExample {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<String> inputData=new ArrayList<>();
        inputData.add("WARN: 4 Sept 1983");
        inputData.add("ERROR: 7 Sept 1983");
        inputData.add("FATAL: 10 Sept 1983");
        inputData.add("ERROR: 30 Sept 1983");
        inputData.add("FATAL: 10 Sept 1983");
        inputData.add("WARN: 21 Sept 1983");
        inputData.add("WARN: 22 Sept 1983");
        inputData.add("ERROR: 23 Sept 1983");
        inputData.add("ERROR: 24 Sept 1983");

        SparkConf conf=new SparkConf().setAppName("Spark Hello World").setMaster("local[3]");
        JavaSparkContext context=new JavaSparkContext(conf);

        //We are using Iterables api from Guava to get the size of pair RDD
        //As GroupByKey gives <Key,Iterable> object and get size we have to use Guava library
        context.parallelize(inputData)
                .mapToPair(msg->new Tuple2<>(msg.split(":")[0],1L))
                .groupByKey()
                .foreach(tuple->System.out.println(tuple._1 + " has " + Iterables.size(tuple._2) + " instances"));
                //.foreach(tuple->System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
        context.close();

    }
}
