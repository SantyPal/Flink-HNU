package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableAsExample {
    public static void main(String[] args) throws Exception {
        // 1️⃣ Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3️⃣ Create a Table with Renamed Columns
        Table peopleTable = tableEnv.fromValues(
                Row.of("Alice", 30),
                Row.of("Bob", 25),
                Row.of("Charlie", 35)
        ).as("full_name", "years_old"); // Renaming columns

        // 4️⃣ Apply select() to choose specific columns
        Table selectedTable = peopleTable.select($("full_name"));

        // 5️⃣ Execute and Print the Table
        selectedTable.execute().print();
    }
}
