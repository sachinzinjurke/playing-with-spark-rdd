package com.spark.tutorial.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class SparkMapping {

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
        //long count = rdd.count();
        //System.out.println("Total Count " + count);
        System.out.println("Num Of Partitions:: " + rdd.getNumPartitions());

        JavaRDD<Integer> singleIntegerRdd= rdd.map(value->1);
        long result=singleIntegerRdd.reduce((val1,val2)->val1 + val2);
        System.out.println("Total Count :: " + result);
        //Collecting RDD into collection to get rid off Not Serializable exception
        singleIntegerRdd.collect().forEach(System.out::println);
        context.close();
        //SparkSession session=new SparkSession();
    }
}
