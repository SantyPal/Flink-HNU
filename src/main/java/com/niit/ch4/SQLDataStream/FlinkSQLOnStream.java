package com.niit.ch4.SQLDataStream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQLOnStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create a DataStream of integers
        DataStream<Integer> numberStream = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);//
       // DataStream<String> str=env.fromElements("1BD1","2BD2","1JV1","2JV2");

        // Convert DataStream to Table with column name value
        Table numberTable = tableEnv.fromDataStream(numberStream, $("value"));

        // Register the table as a temporary view
        tableEnv.createTemporaryView("Numbers", numberTable);

        // Run an SQL Query
        Table evenNumbers = tableEnv.sqlQuery("SELECT `value` FROM Numbers WHERE MOD(`value`, 2) = 0");

        // Convert Table back to DataStream and print the results
        tableEnv.toDataStream(evenNumbers).print();

        env.execute();
    }
}
