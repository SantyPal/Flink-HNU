package com.niit.ch4.Operators;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//Since Flink no longer has BatchTableEnvironment, enable batch mode in StreamTableEnvironment before applying limit().

public class FlinkLimitExample {
    public static void main(String[] args) throws Exception {
        // 1. Create Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable batch mode (required for LIMIT)
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inBatchMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. Create a Table with sample data
        Table employees = tableEnv.fromValues(
                Row.of(1, "Alice", 3000),
                Row.of(2, "Bob", 5000),
                Row.of(3, "Charlie", 4000),
                Row.of(4, "David", 2000)
        ).as("id", "name", "salary");

        // 3. Apply LIMIT to get only 2 rows
        //select * from employees top 2
        Table limitedEmployees = employees.limit(2);

        // 4. Execute and Print Results
        limitedEmployees.execute().print();
    }
}
