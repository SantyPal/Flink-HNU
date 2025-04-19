package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkFullOuterJoinSQLExample {
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
                Row.of(3, "Charlie", 103),
                Row.of(4, "David", 104) // No matching dept_id
        ).as("emp_id", "emp_name", "emp_dept_id"); // Renaming columns

        // 4. Create Department Table
        Table departments = tableEnv.fromValues(
                Row.of(101, "HR"),
                Row.of(102, "IT"),
                Row.of(103, "Finance"),
                Row.of(105, "Marketing") // Department without employees
        ).as("dept_id", "dept_name");

        // 5. Register Tables in Table Environment
        tableEnv.createTemporaryView("Employees", employees);
        tableEnv.createTemporaryView("Departments", departments);

        // 6. Perform Full Outer Join Using SQL
        Table resultTable = tableEnv.sqlQuery(
                "SELECT e.emp_name, d.dept_name " +
                        "FROM Employees AS e " +
                        "FULL OUTER JOIN Departments AS d " +
                        "ON e.emp_dept_id = d.dept_id"
        );

        // 7. Execute and Print Results
        resultTable.execute().print();
    }
}
