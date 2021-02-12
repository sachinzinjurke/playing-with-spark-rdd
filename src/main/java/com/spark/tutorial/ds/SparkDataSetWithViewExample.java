package com.spark.tutorial.ds;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkDataSetWithViewExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName(" Spark DataSet Sql Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/subtitles/students.csv");
        dataset.createOrReplaceTempView("student_view");
       // Dataset<Row> result = spark.sql("select * from student_view where subject='French'");
        Dataset<Row> result = spark.sql("select score,year,subject from student_view where subject='French'");
        result.show();
        spark.close();
    }
}
