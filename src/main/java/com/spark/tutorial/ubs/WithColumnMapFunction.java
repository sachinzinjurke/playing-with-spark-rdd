package com.spark.tutorial.ubs;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WithColumnMapFunction implements MapFunction<Person, Row> {
    @Override
    public Row call(Person person) throws Exception {
        return RowFactory.create(person.getName(),
                person.getSurname(),
                person.getAge(),
                person.getSalary(),
               getDateFormatted(person.getBdate())
               );
    }

    public String getDateFormatted(Date date){
        DateFormat dateFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
        String strDate = dateFormat.format(date);
        return strDate;
    }
}
