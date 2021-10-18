package com.spark.tutorial.ds;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;


public class SparkDataSetUnionExample {

    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        //System.setProperty("hadoop.home.dir","C://softwares//hadoop-common-2.2.0-bin-master");
        SparkSession spark= SparkSession.builder().appName("Dataset Test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        Dataset<Row> union=null;
        String[] studentFileName=new String[]{
          "students.csv",
          "students1.csv",
          "students2.csv",
                "students3.csv"

        };
        for (String file:studentFileName) {

            Dataset<Row> ds = read(file, spark);
            if(union==null){
                union=ds;
                System.out.println("Union null : " + ds.count()) ;
                System.out.println("Unperstisting DS : " + ds.unpersist()) ;
            }else if(ds!=null){
               union= union.union(ds);
                System.out.println("Union nit null " + union.count());
                System.out.println("Unperstisting DS : " + ds.unpersist()) ;
            }
            System.out.println("Union Count : " + union.count());
        }


        spark.close();
    }

    private static Dataset<Row>  read(String file, SparkSession spark) {
        String path="src/main/resources/subtitles/";
        StringBuilder fileName = new StringBuilder()
                .append(path).append(file);
        Dataset<Row> ds=null;
        try{
             ds = spark.read().option("header", true).csv(fileName.toString());
        }catch(Exception ex){
            System.out.println("Exception occurred while reading from path : " + file);
        }
        return ds;
    }
}
