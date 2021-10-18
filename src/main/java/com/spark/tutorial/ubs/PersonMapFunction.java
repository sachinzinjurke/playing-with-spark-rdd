package com.spark.tutorial.ubs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.math.BigDecimal;

public class PersonMapFunction implements MapFunction<Row,Row> {

    int rowId=0;
    @Override
    public Row call(Row row) throws Exception {
        rowId++;
        return RowFactory.create(generateFileName(),
                this.rowId,row.getAs("name"),
                row.getAs("surname"),
                row.getAs("age"),
                String.valueOf(row.getAs("salary")),
                String.valueOf(row.getAs("birthDate")),
                "location");
    }

    public String generateFileName(){
        return "FileName";
    }
}
