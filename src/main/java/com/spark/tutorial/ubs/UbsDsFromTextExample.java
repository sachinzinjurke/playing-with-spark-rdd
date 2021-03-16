package com.spark.tutorial.ubs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;


public class UbsDsFromTextExample {

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

        List<Row> rowList = new ArrayList<Row>();
       // Row row = RowFactory.create(personList.toArray());
        for (Person p:personList) {
            Row row = RowFactory.create(p.getName(),p.getSurname(),p.getAge());
            rowList.add(row);
        }


        System.out.println("Row List :: " + rowList);
        List<DataType> datatype = new ArrayList<DataType>();
        datatype.add(DataTypes.StringType);
        datatype.add(DataTypes.StringType);
        datatype.add(DataTypes.IntegerType);

        List<String> headerList = new ArrayList<String>();
        headerList.add("Custom_First_Name");
        headerList.add("Custom_Last_Name");
        headerList.add("Custom_Age");

        StructField structField1 = new StructField(headerList.get(0), datatype.get(0), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField2 = new StructField(headerList.get(1), datatype.get(1), true, org.apache.spark.sql.types.Metadata.empty());
        StructField structField3 = new StructField(headerList.get(2), datatype.get(2), true, org.apache.spark.sql.types.Metadata.empty());

        List<StructField> structFieldsList = new ArrayList<>();
        structFieldsList.add(structField1);
        structFieldsList.add(structField2);
        structFieldsList.add(structField3);

        StructType schema = DataTypes.createStructType(structFieldsList);

        System.out.println("Schema Create :: " + schema);
        Dataset<Row> ds = spark.createDataFrame(rowList, schema);
        ds.coalesce(1).write().option("header","true").csv("src/main/resources/ubs/out/personOut.txt");
        ds.show();

        ds.toJavaRDD().saveAsTextFile("src/main/resources/ubs/out/rdd/rddPerson.txt");
       // System.out.println(rowList);
        spark.close();
    }
}
