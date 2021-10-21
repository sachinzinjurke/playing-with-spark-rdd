package com.spark.tutorial.streams;

import com.spark.tutorial.functions.ColtForEachBatch;
import com.spark.tutorial.functions.MorForEachBatch;
import com.spark.tutorial.partition.EmployeePartitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class SparkTextFileStucturedStreamingExample {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder()
                            .appName("spark streaming").config("spark.master", "local")
                            .config("spark.sql.warehouse.dir", "file:///app/").getOrCreate();
        spark.conf().set("spark.sql.streaming.metricsEnabled", "true");
        spark.conf().set("spark.sql.shuffle.partitions", "4");
        spark.conf().set("spark.sql.streaming.minBatchesToRetain",2);

        //define schema type of file data source
        StructType schema = new StructType()
                .add("empId", DataTypes.StringType,false)
                .add("empName", DataTypes.StringType)
                .add("department", DataTypes.StringType)
                .add("Join date", DataTypes.StringType)
                .add("Filter Reason", DataTypes.StringType)
                .add("GPN", DataTypes.StringType);

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
                //.option("mode", "DROPMALFORMED")
                .option("badRecordsPath", "D:\\spark\\streaming\\bad\\")
                .option("inferSchema","true")
                .schema(schema)
                .csv("D:\\spark\\streaming\\text\\");
               // .withColumn("FileName", input_file_name());

      //  rawData.createOrReplaceTempView("empData");
        //count of employees grouping by department
        //Dataset<Row> result = spark.sql("select count(*), department from  empData group by department");

        Dataset<Row> dateDs = rawData
                .withColumn("processing_ts", to_utc_timestamp(to_timestamp(col("Join date"), "yyyy/MM/dd HH:mm:ss"), "UTC"))
                .withColumn("processing_ts_1", to_timestamp(col("Join date"), "yyyy/MM/dd HH:mm:ss"))
                .withColumn("master_id", lit("8").cast(DataTypes.IntegerType))
                .withColumn("version", lit("5").cast(DataTypes.IntegerType))
                .withColumnRenamed("department", "new_dept")
                .withColumn("new_dept",col("new_dept").cast(DataTypes.IntegerType))
                .withColumn("inventory_key", concat(col("empId"),lit("_"),col("master_id")))
                .dropDuplicates("inventory_key");

                //.filter(col("Filter Reason").isNull());
        rawData.printSchema();


       // dateDs.printSchema();
        Dataset<Row> filter = rawData.filter(col("empName").equalTo("Sachin"));
        //write stream to output console with update mode as data is being aggregated
        StreamingQuery query = dateDs
                .writeStream()
                .format("console")
                .foreachBatch(new ColtForEachBatch())
                .outputMode(OutputMode.Append())
                .trigger(Trigger.ProcessingTime("1 minute"))
                .option("checkpointLocation", "D:\\spark\\streaming\\checkpoint\\")
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
