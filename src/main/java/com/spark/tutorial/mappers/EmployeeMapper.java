package com.spark.tutorial.mappers;

import com.spark.tutorial.pojo.Employee;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

public class EmployeeMapper implements MapFunction<Row, Employee> {
    @Override
    public Employee call(Row row) throws Exception {
       // System.out.println("Called ...");
        System.out.println("Mapper : " + Thread.currentThread().getName());
        return new Employee(row.getString(0),row.getString(1),row.getString(2));
    }
}
