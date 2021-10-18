package com.spark.tutorial.ubs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;


public class UbsRddEncoderExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName("Dataset Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        List<Person> personList=new ArrayList<>();
        for(int i=7;i<17;i++){
            Person p=new Person("Name-" + i,"Surname-"+i ,i,getRandomNumber(i));
            personList.add(p);
        }

        JavaSparkContext context=new JavaSparkContext(spark.sparkContext());
        JavaRDD<Person> personRDD = context.parallelize(personList);

        //JavaRDD<Row> personRowRDD = personRDD.map(p -> RowFactory.create(p.getName(), p.getSurname(), p.getAge(),"location"));
        Dataset<Row> personDS = spark.createDataFrame(personRDD, Person.class).coalesce(1);
        ExpressionEncoder<Row> encoder = RowEncoder.apply(StructUtil.personSchema);

       // personDS.show();
        Dataset<Row> personWithLocationDS = personDS.map(new PersonMapFunction(), encoder);

        System.out.println("Schema Create :: " + StructUtil.personSchema);
        //Dataset<Row> ds = spark.createDataFrame(personRowRDD, StructUtil.personSchema);
        personWithLocationDS.coalesce(1).write().option("header","true").csv("src/main/resources/ubs/rdd/encoder/");
        personWithLocationDS.show();

       // ds.toJavaRDD().saveAsTextFile("src/main/resources/ubs/out/rdd/rddPerson.txt");
       // System.out.println(rowList);
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
