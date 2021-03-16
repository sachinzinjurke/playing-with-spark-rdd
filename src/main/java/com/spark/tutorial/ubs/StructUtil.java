package com.spark.tutorial.ubs;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class StructUtil {

    public static StructType personSchema = new StructType(new StructField[]{
            new StructField("FileName", DataTypes.StringType, false, Metadata.empty()),
            new StructField("RowId", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("Name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("Surname", DataTypes.StringType, false, Metadata.empty()),
            new StructField("Age", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("Location", DataTypes.StringType, false, Metadata.empty()),
    });
}
