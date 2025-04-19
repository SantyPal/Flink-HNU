package com.niit.ch4;

import org.apache.flink.table.api.*;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkBatchTableExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment (even for batch mode)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Optional: for predictable output

        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create an input Table using the Table API
        Table peopleTable = tableEnv.fromValues(
                Row.of("Alice", 30),
                Row.of("Bob", 25)
        ).as("name", "age");

        // 4. Register the table
        tableEnv.createTemporaryView("People", peopleTable);

        // 5. Run a SQL query
        Table resultTable = tableEnv.sqlQuery("SELECT name, age FROM People WHERE age >= 30");

        // 6. Print the results
        resultTable.execute().print();
    }
}
