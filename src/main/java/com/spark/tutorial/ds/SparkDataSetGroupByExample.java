package com.spark.tutorial.ds;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkDataSetGroupByExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName(" Spark DataSet Sql Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/subtitles/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");
        Dataset<Row> groupByDs = spark.sql("select level,date_format(datetime,'MMMM') as month, count(1) as total from logging_table group by level,month");
        groupByDs.show(100);
        groupByDs.createOrReplaceTempView("result_total");
        Dataset<Row> totalCountDs = spark.sql("select sum(total) from result_total");
        totalCountDs.show();
        spark.close();
    }
}
