package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkFilterExample {
        public static void main(String[] args) throws Exception {
            // Set up the execution environment
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

            // Create a table with sample data
            Table employees = tableEnv.fromValues(
                    Row.of(1, "Alice", 5000.0),
                    Row.of(2, "Bob", 6000.0),
                    Row.of(3, "Charlie", 7000.0)
            ).as("emp_id", "emp_name", "emp_salary");

            // Apply filter to select employees with salary greater than 5500
            //select emp_name from employees where emp_salary > 5500;
            Table filteredTable = employees.filter($("emp_salary").isLess(5500)) //where emp_salary > 5500
                    .select($("emp_name"),$("emp_salary"));             //select emp_name

           // Print the result table
            filteredTable.execute().print();
        }
    }
