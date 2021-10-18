package com.spark.tutorial.ubs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.beans.Encoder;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class UbsRddEncoderWithColumnExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName("Dataset Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        List<Person> personList=new ArrayList<>();
        for(int i=7;i<17;i++){
            Person p=new Person("Name-" + i,"Surname-"+i ,i,getRandomNumber(i),new Date());
            personList.add(p);
        }

        JavaSparkContext context=new JavaSparkContext(spark.sparkContext());
        JavaRDD<Person> personRDD = context.parallelize(personList);

        //JavaRDD<Row> personRowRDD = personRDD.map(p -> RowFactory.create(p.getName(), p.getSurname(), p.getAge(),"location"));
        Dataset<Person> personDS=spark.createDataset(personRDD.rdd(), Encoders.bean(Person.class));

        Dataset<Row> personDSWithRename = personDS.withColumnRenamed("name", "Pname")
                .withColumnRenamed("surname", "pSurname")
                .withColumnRenamed("salary", "pSalary")
                .withColumnRenamed("age", "Page")
                .withColumnRenamed("bdate", "PersonDate");


        personDSWithRename.coalesce(1).write().option("header","true").csv("src/main/resources/ubs/rdd/encoder/");
        personDSWithRename.show();
        spark.close();
    }

    public static BigDecimal getRandomNumber(int range) {
        BigDecimal max = new BigDecimal(range);
        BigDecimal randFromDouble = new BigDecimal(Math.random());
        BigDecimal actualRandomDec = randFromDouble.multiply(max);
        actualRandomDec = actualRandomDec
                .setScale(2, BigDecimal.ROUND_DOWN);
        return actualRandomDec;
    }
}
