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

import java.util.ArrayList;
import java.util.List;


public class UbsRddEncoderExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName("Dataset Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        List<Person> personList=new ArrayList<>();
        for(int i=0;i<10;i++){
            Person p=new Person("Name-" + i,"Surname-"+i ,i);
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
        personWithLocationDS.coalesce(1).write().option("header","true").csv("src/main/resources/ubs/rdd/encoder/personOut.txt");
        personWithLocationDS.show();

       // ds.toJavaRDD().saveAsTextFile("src/main/resources/ubs/out/rdd/rddPerson.txt");
       // System.out.println(rowList);
        spark.close();
    }
}
