package com.spark.tutorial.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class SparkHelloWorld {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        List<Double> inputData=new ArrayList<>();
        inputData.add(3.5);
        inputData.add(2.7);
        inputData.add(10.5);
        inputData.add(25.7);
        inputData.add(33.4);
        inputData.add(23.7);

        SparkConf conf=new SparkConf().setAppName("Spark Hello World").setMaster("local[3]");
        JavaSparkContext context=new JavaSparkContext(conf);

        JavaRDD<Double> rdd = context.parallelize(inputData);
        long count = rdd.count();
        System.out.println("Total Count " + count);
        System.out.println("Num Of Partitions:: " + rdd.getNumPartitions());
        Double reduce = rdd.reduce((val1, val2) -> {
            System.out.println("Thread Name :: " + Thread.currentThread().getName());
            return val1 + val2;
        });
        System.out.println(reduce);

        //rdd.foreach(tVoidFunction);
        context.close();
        //SparkSession session=new SparkSession();
    }
}
