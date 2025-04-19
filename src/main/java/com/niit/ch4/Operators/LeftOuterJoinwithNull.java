package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class LeftOuterJoinwithNull {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        Table employees = tableEnvironment.fromValues(
                DataTypes.ROW(
                      DataTypes.FIELD("emp_id", DataTypes.INT().nullable()),
                      DataTypes.FIELD("name", DataTypes.STRING().nullable()),
                      DataTypes.FIELD("dept_id", DataTypes.INT().nullable())
                ),
                Row.of(1, "a", 1000),
                Row.of(2, "b", 1001),
                Row.of(3, "c", 1002))
                .as("emp_id", "emp_name", "emp_dpt_id");
        Table department = tableEnvironment.fromValues(
                DataTypes.ROW(
                      DataTypes.FIELD("dpt id", DataTypes.INT().nullable()),
                      DataTypes.FIELD("dpt_name", DataTypes.STRING().nullable())
                ),
                Row.of(1000, "sales"),
                Row.of(1001, "market"))
                .as("dpt_id", "dpt_name");
        Table emp_dpt = employees.leftOuterJoin(department, $("emp_dpt_id").isEqual($("dpt_id")));
        emp_dpt.execute().print();
    }
}