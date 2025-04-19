package com.niit.ch4.SQLDataStream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLOnStreamString {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create a DataStream of strings
        DataStream<String> numberStream = env.fromElements("BD1", "BD2", "JV1", "JV2");

        // Convert DataStream to Table with a column named "value"
        Table numberTable = tableEnv.fromDataStream(numberStream, $("value"));

        // Register the table as a temporary view
        tableEnv.createTemporaryView("Numbers", numberTable);

        // SQL Query to filter values containing 'BD'
        Table filteredTable = tableEnv.sqlQuery("SELECT `value` FROM Numbers WHERE `value` LIKE '%BD%'");

        // Convert the result Table back to DataStream and print
        tableEnv.toDataStream(filteredTable).print();

        env.execute();
    }
}
