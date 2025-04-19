package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkTableGroupByExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create an input Table
        Table peopleTable = tableEnv.fromValues(
                Row.of("Alice", 30,"BD"),           //Table class can process each row using map()
                Row.of("Bob", 25,"JV"),
                Row.of("Charlie", 30,"BD"),
                Row.of("David", 25,"JV"),
                Row.of("Eve", 35,"BD")
        ).as("name", "age","major");

        Table peopleTable2 = tableEnv.fromValues(
                Row.of("Alice", 30,"BD"),           //Table class can process each row using map()
                Row.of("Bob", 25,"JV"),
                Row.of("Charlie", 30,"BD"),
                Row.of("David", 25,"JV"),
                Row.of("Eve", 35,"BD")
        ).as("name", "age","major");

        // 4. Perform groupBy and aggregate (count people in each age group)
        //select major, count(*) from student group by(major);
        Table groupedTable=peopleTable.groupBy($("major")).select($("major"),$("name").count());

        // 5. Execute and print the results
        groupedTable.execute().print();
    }
}