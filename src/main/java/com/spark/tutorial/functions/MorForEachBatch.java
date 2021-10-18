package com.spark.tutorial.functions;

import com.spark.tutorial.mappers.EmployeeMapPartition;
import com.spark.tutorial.mappers.EmployeeMapper;
import com.spark.tutorial.pojo.Employee;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class MorForEachBatch implements VoidFunction2<Dataset<Row>,Long> {
    @Override
    public void call(Dataset<Row> ds, Long batch) throws Exception {
        System.out.println(" DataSet started for batch : " +batch);
        //Dataset<Employee> empoyeeDs = ds.map(new EmployeeMapper(), Encoders.bean(Employee.class));
        //empoyeeDs.foreach((ForeachFunction<Employee>) emp-> System.out.println(emp));
        System.out.println("**** before Partition Count :: " + ds.rdd().getNumPartitions());
        Dataset<Row> dsRepartitioned = ds.repartition(3);
        System.out.println("****after re Partition Count :: " + dsRepartitioned.rdd().getNumPartitions());
        Dataset<Employee> empDs = dsRepartitioned.mapPartitions(new EmployeeMapPartition(), Encoders.bean(Employee.class));
        empDs.foreach((ForeachFunction<Employee>) emp-> System.out.println(emp));
        // ds.foreach((ForeachFunction<Row>) row-> System.out.println(Thread.currentThread().getName() + " " + row.getString(1)));
    }


}
