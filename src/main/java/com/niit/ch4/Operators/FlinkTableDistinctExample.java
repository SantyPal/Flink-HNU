package com.niit.ch4.Operators;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkTableDistinctExample {
    public static void main(String[] args) throws Exception {
        // 1. Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. Create a Table with duplicate values
        Table wordsTable = tableEnv.fromValues(
                Row.of("Flink"),
                Row.of("Java"),
                Row.of("Flink"),
                Row.of("Spark"),
                Row.of("Java"),
                Row.of("Hadoop"),
                Row.of("Spark"),
                Row.of("Flink")
        ).as("word"); // Define column name

        // 3. Apply distinct transformation
        //select distinct(*) from wordTable
        Table distinctTable = wordsTable.distinct();

        // 4. Execute and print results
        distinctTable.execute().print();
    }
}
