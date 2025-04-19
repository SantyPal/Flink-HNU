package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableJoinExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create Employee Table with explicit column names
        Table employees = tableEnv.fromValues(
                Row.of(1, "Alice", 101),
                Row.of(2, "Bob", 102),
                Row.of(3, "Charlie", 103),
                Row.of(4, "David", 101),
                Row.of(5, "Eve", 104)
        ).as("emp_id", "emp_name", "emp_dept_id"); // Renaming columns

        // 4. Create Department Table with explicit column names
        Table departments = tableEnv.fromValues(
                Row.of(101, "HR"),
                Row.of(102, "IT"),
                Row.of(103, "Finance")
        ).as("dept_id", "dept_name");
        //Alice HR
        //BOb IT
        //3                                       1              2
        //select name,dept_name from employees join departmenets on(where) employees.emp_dept_id = departments.dept_id;

        // 5. Perform Inner Join (Using Renamed Column `emp_dept_id`)
        //select emp_name, dept_name from employees join departments on employees.emp_dept_id = departments.dept_id;
         Table joinedTable = employees.join(departments)            //employees join departments
                .where($("emp_dept_id").isEqual($("dept_id"))) // Resolving ambiguity //on employees.emp_dept_id = departments.dept_id;
                .select($("emp_name"), $("dept_name")); //select emp_name, dept_name

        // 6. Execute and print the results
        joinedTable.execute().print();
    }
}
