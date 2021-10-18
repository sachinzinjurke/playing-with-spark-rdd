package com.spark.tutorial.mappers;

import com.spark.tutorial.pojo.Employee;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class EmployeeMapPartition implements MapPartitionsFunction<Row, Employee> {
    @Override
    public Iterator<Employee> call(Iterator<Row> iterator) throws Exception {
        System.out.println("Partition Mapper : " + Thread.currentThread().getName());
        List<Employee> employeeList = new ArrayList<>();
        while(iterator.hasNext()){
            employeeList.add(transform(iterator.next()));
        }
        return employeeList.iterator();
    }

    private Employee transform(Row row) {
        return new Employee(row.getString(0),row.getString(1),row.getString(2));
    }
}
