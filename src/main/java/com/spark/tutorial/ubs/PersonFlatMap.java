package com.spark.tutorial.ubs;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class PersonFlatMap implements FlatMapFunction<Row,Row> {
    @Override
    public Iterator call(Row row) throws Exception {
        List<Object> data = new ArrayList<>();
        data.add(row.getString(0));
        data.add(row.getString(1));
        data.add(row.getInt(2));
        data.add("location");
        ArrayList<Row> list = new ArrayList<>();
        list.add(RowFactory.create(data.toArray()));
        return list.iterator();
    }
}
