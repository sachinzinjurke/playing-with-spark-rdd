package com.spark.tutorial.functions;

import com.spark.tutorial.mappers.EmployeeMapPartition;
import com.spark.tutorial.partition.EmployeePartitioner;
import com.spark.tutorial.pojo.Employee;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.row_number;

public class ColtForEachBatch implements VoidFunction2<Dataset<Row>,Long> {
    @Override
    public void call(Dataset<Row> ds, Long batch) throws Exception {
        System.out.println(" DataSet started for batch : " +batch  );
        Dataset<Row> persist = ds.persist();
        persist.show();
        System.out.println("Dataset no of records : " + persist.count());
        persist.unpersist();
        if(ds.count() > 0){
            Row fileName = ds.select(col("FileName")).first();
            System.out.println("File Name : " + fileName);
            System.out.println("Total num of partitions: " + ds.rdd().getNumPartitions());

            Dataset<Row> inKeyDs = ds.filter(col("inventory_key").isNotNull());
            inKeyDs.foreachPartition(new EmployeePartitioner());
        }

    }


}
