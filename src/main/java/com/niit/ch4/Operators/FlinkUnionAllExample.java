package com.niit.ch4.Operators;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkUnionAllExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create Employee Table
        Table employees = tableEnv.fromValues(
                Row.of(1, "Alice", 101),
                Row.of(2, "Bob", 102),
                Row.of(3, "Charlie", 103)
        ).as("emp_id", "emp_name", "emp_dept_id");

        // 4. Create Contractor Table
        Table contractors = tableEnv.fromValues(
                Row.of(101, "Eve", 104),
                Row.of(102, "Frank", 105),
                Row.of(103, "Grace", 106),
                Row.of(3, "Charlie", 103) // Duplicate row (same as in `employees` table)
        ).as("emp_id", "emp_name", "emp_dept_id");

        // 5. Union Employees and Contractors Tables Using `unionAll()`
        Table resultTable = employees.unionAll(contractors);

        // 6. Execute and Print Results
        resultTable.execute().print();
    }
}