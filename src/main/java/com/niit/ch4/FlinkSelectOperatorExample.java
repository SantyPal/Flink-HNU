package com.niit.ch4;

import org.apache.flink.table.api.*;

public class FlinkSelectOperatorExample {
    public static void main(String[] args) {
        // Create TableEnvironment in batch mode
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Register a table with sample data using the VALUES connector (for small static datasets)
        tableEnv.executeSql(
                "CREATE TEMPORARY TABLE people (" +
                        "  name STRING, " +
                        "  age INT " +
                        ") WITH (" +
                        "  'connector' = 'values' " + // Values connector for static data print
                        ")"
        );

        // Insert sample data into the 'people' table
        tableEnv.executeSql("INSERT INTO people VALUES ('Alice', 25), ('Bob', 30), ('Charlie', 35)");

        // Perform a SELECT operation (simple query)
        Table result = tableEnv.sqlQuery("SELECT name, age FROM people WHERE age > 25");

        // Print the results of the SELECT query
        result.execute().print(); // Outputs only Bob and Charlie as their age > 25
    }
}