package com.spark.tutorial.ubs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class UbsReadFromEncoderFileExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName("Dataset Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> ds = spark.read()
                .option("header", true)
                .format("org.apache.spark.csv")
                .option("inferSchema", true)
                .csv("src/main/resources/ubs/rdd/encoder/");
        System.out.println("Printing dataset");
        ds.show();
        RDD<Row> rdd = ds.rdd();
        System.out.println("RDD " + rdd.partitions().length);
        System.out.println(rdd);
        spark.close();
    }
}
