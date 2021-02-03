package com.spark.tutorial.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkTupleExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Integer> inputData=new ArrayList<>();
        inputData.add(5);
        inputData.add(10);
        inputData.add(15);
        inputData.add(20);
        inputData.add(25);
        inputData.add(30);

        SparkConf conf=new SparkConf().setAppName("Spark Hello World").setMaster("local[3]");
        JavaSparkContext context=new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = context.parallelize(inputData);
        JavaRDD<Double> sqrtRdd = rdd.map(value -> Math.sqrt(value));

        JavaRDD<Tuple2<Integer,Double>> tupleRdd = rdd.map(value -> new Tuple2<>(value, Math.sqrt(value)));

        tupleRdd.collect().forEach(System.out::println);
        context.close();
        //SparkSession session=new SparkSession();
    }
}
