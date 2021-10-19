package com.spark.tutorial.streams;

import com.spark.tutorial.functions.MorForEachBatch;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

public class SparkTextFileStucturedStreamingExample {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                            .appName("spark streaming").config("spark.master", "local")
                            .config("spark.sql.warehouse.dir", "file:///app/").getOrCreate();

        //define schema type of file data source
        StructType schema = new StructType()
                .add("empId", DataTypes.StringType)
                .add("empName", DataTypes.StringType)
                .add("department", DataTypes.StringType);

        //build the streaming data reader from the file source, specifying csv file format
        Dataset<Row> rawData = spark
                .readStream()
                .option("header", true)
                //.format("txt")
                .option("sep", "|")
                .option("cleanSource","archive")
                .option("spark.sql.streaming.fileSource.log.compactInterval","1")
                .option("spark.sql.streaming.fileSource.log.cleanupDelay","5")
                .option("sourceArchiveDir","D:\\spark\\streaming\\archive\\")
                .schema(schema)
                .csv("D:\\spark\\streaming\\text\\");

      //  rawData.createOrReplaceTempView("empData");
        //count of employees grouping by department
        //Dataset<Row> result = spark.sql("select count(*), department from  empData group by department");


        //write stream to output console with update mode as data is being aggregated
        StreamingQuery query = rawData
                .writeStream()
                .format("console")
                .outputMode(OutputMode.Update())
                .start();

        /*StreamingQuery query;
        query = rawData
                .writeStream()
                .outputMode(OutputMode.Update())
                .foreachBatch(new MorForEachBatch())
                .start();*/
        query.awaitTermination();
        if(query.isActive()){
            System.out.println("Is Running..");
        }

    }
}
