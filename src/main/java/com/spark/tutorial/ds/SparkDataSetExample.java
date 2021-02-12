package com.spark.tutorial.ds;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class SparkDataSetExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName("Dataset Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> studentsDataset = spark.read().option("header", true).csv("src/main/resources/subtitles/students.csv");
        /*Column subjectCol= functions.col("subject");
        Column yearCol=functions.col("year");*/
        //We can use Column syntax as above commented OR we can use col() method from functions class..imported statically as below
        Dataset<Row> modern_art_students_ds = studentsDataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq("2007")));;
        modern_art_students_ds.show();
        spark.close();
    }
}
