package com.spark.tutorial.partition;

import com.spark.tutorial.ubs.Inventory;
import com.spark.tutorial.ubs.Person;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Row;

import java.util.Iterator;

public class EmployeePartitioner implements ForeachPartitionFunction<Row> {
    @Override
    public void call(Iterator<Row> iterator) throws Exception {
        System.out.println("*************************ROW : " );
        iterator.forEachRemaining(row -> {
            System.out.println(row);
            Inventory inv = toDomain(row);
            System.out.println(inv);

        });
    }

    private Inventory toDomain(Row row) {

        Inventory p = new Inventory();
        p.setInventoryId(row.getAs("empId"));
        p.setVersion(row.getAs("version"));
        p.setTs(row.getAs("processing_ts_1"));
        p.setGpnId(row.getAs("GPN"));
        p.setMasterId(row.getAs("master_id"));
        p.setInventoryKey(row.getAs("inventory_key"));
        return p;
    }
}
