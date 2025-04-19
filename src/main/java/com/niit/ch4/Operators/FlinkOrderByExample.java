package com.niit.ch4.Operators;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

//Flink is designed to process streaming data, where ordering on arbitrary fields (like salary) is not directly supported unless you work in batch mode or use an event-time field.
public class FlinkOrderByExample {
    public static void main(String[] args) throws Exception {
        // 1. Create Execution Environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Create a Table with sample data
        Table employees = tableEnv.fromValues(
                Row.of(1, "Alice", 3000),
                Row.of(2, "Bob", 5000),
                Row.of(3, "Charlie", 4000),
                Row.of(4, "David", 2000)
        ).as("id", "name", "salary"); // Define column names

        // 3. Apply ORDER BY transformation
        Table sortedEmployees = employees.orderBy($("salary").desc());

        // 4. Execute and Print Results
        sortedEmployees.execute().print();
    }
}
