package com.spark.tutorial.ubs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class PersonMapFunction implements MapFunction<Row,Row> {
    @Override
    public Row call(Row row) throws Exception {
        return RowFactory.create(row.getAs("name"),row.getAs("surname"),row.getAs("age"),"location");
    }
}
