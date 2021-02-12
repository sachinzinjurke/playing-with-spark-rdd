package com.spark.tutorial.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkPairRddExample {
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

        JavaRDD<String> messageRdd = context.parallelize(inputData);
        JavaPairRDD<String, Long> msgPairRdd = messageRdd.mapToPair(msg -> {
            String[] split = msg.split(":");
            return new Tuple2<>(split[0], 1L);
        });
        Function2<Long,Long,Long> reduceFunction=(val1, val2) ->{
            System.out.println("Got Val1 : " + val1 + " Val2 : " + val2);
            return val1 + val2;
        } ;
        JavaPairRDD<String, Long> sumRdd = msgPairRdd.reduceByKey(reduceFunction);
        sumRdd.foreach(tuple->System.out.println(tuple._1 + " has " + tuple._2 + " instances"));

        context.close();
        //SparkSession session=new SparkSession();
    }
}
