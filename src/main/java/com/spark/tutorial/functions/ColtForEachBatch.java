package com.spark.tutorial.functions;

import com.spark.tutorial.mappers.EmployeeMapPartition;
import com.spark.tutorial.partition.EmployeePartitioner;
import com.spark.tutorial.pojo.Employee;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

public class ColtForEachBatch implements VoidFunction2<Dataset<Row>,Long> {
    @Override
    public void call(Dataset<Row> ds, Long batch) throws Exception {
        System.out.println(" DataSet started for batch : " +batch  );
        System.out.println("Total num of partitions: " + ds.rdd().getNumPartitions());
        ds.foreachPartition(new EmployeePartitioner());
    }


}
