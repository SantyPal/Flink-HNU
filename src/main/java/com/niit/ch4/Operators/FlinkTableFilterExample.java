package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableFilterExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // For predictable results

        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create an input Table with data
        Table peopleTable = tableEnv.fromValues(
                Row.of("Alice", 30),
                Row.of("Bob", 25),
                Row.of("Charlie", 35)
        ).as("name", "age");

        // 4. Apply a filter operation: Select people whose age is >= 30
        Table filteredTable = peopleTable.filter($("age").isGreaterOrEqual(30));

        // 5. Print the results
        filteredTable.execute().print();
    }
}
