package com.spark.tutorial.ds;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class SparkDataSetWithUserDefinedFunctionExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName(" Spark DataSet Sql Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/subtitles/students.csv");

        //to add new column in DS on the fly
        //Added static import for lit and col methods from functions class
        Dataset<Row> result = dataset.withColumn("pass", lit(col("grade").equalTo("A+")));
        //result.show();
        //Now will see how we can achieve same functionality with UDF. In UDF we get chance to add complex logic in lambda fun, which is NOT possible
        //using earlier lit approach. We can pass multiple columns to function to add more complex functionality.
        spark.udf().register("hasPassed", (String grade)->{
                if(grade.equals("A+")){
                    return "DIST";
                }else if(grade.equals("B")){
                    return "FIRST CLASS";
                }else if(grade.equals("C")){
                    return "PASSED";
                }else{
                    return "FAILED";
                }
                },
                DataTypes.StringType);
        Dataset<Row> resultWithUDF = dataset.withColumn("pass", callUDF("hasPassed", col("grade")));
        resultWithUDF.show();
        spark.close();
    }
}
