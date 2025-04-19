package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableLeftOuterJoinExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer", "DROP");
        // 3. Create Employee Table Using VALUES
        Table employees = tableEnv.fromValues(
                Row.of(1, "Alice", 101),
                Row.of(2, "Bob", 102),
                Row.of(3, "Charlie", 103),
                Row.of(4, "David", 104) // No matching dept_id
        ).as("emp_id", "emp_name", "emp_dept_id"); // Renaming columns

        // 4. Create Department Table Using VALUES
        Table departments = tableEnv.fromValues(
                Row.of(101, "HR"),
                Row.of(102, "IT"),
                Row.of(103, "Finance")
        ).as("dept_id", "dept_name");

        // 5. Register Tables in Table Environment
        tableEnv.createTemporaryView("Employees", employees);
        tableEnv.createTemporaryView("Departments", departments);

        // 6. Perform Left Outer Join Using SQL
//        Table resultTable = tableEnv.sqlQuery(
//                "SELECT e.emp_name, d.dept_name " +
//                        "FROM Employees AS e " +
//                        "LEFT OUTER JOIN Departments AS d " +
//                        "ON e.emp_dept_id = d.dept_id"
//        );
        Table resultTable = employees.leftOuterJoin(departments, $("emp_dept_id").isEqual($("dept_id")))
                .select($("emp_name"), $("dept_name"));

        // 7. Execute and Print Results
        resultTable.execute().print();
    }
}