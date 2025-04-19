package com.niit.ch4.SQLDataStream;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkKafkaSQLExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(2);
        // Define Kafka Source Table
        String createSourceTable = "CREATE TABLE kafka_source (" +
                " id INT, " +
                " name STRING, " +
                " ts TIMESTAMP(3), " +
                " WATERMARK FOR ts AS ts - INTERVAL '5' SECOND " +
                ") WITH (" +
                " 'connector' = 'kafka', " +        //producer topic
                " 'topic' = 'input_topic', " +      //for sending the messages
                " 'properties.bootstrap.servers' = 'niit:9092', " +         //kafka broker - kafka server address
                " 'properties.group.id' = 'flink_consumer_group', " +
                " 'scan.startup.mode' = 'earliest-offset', " + // optional
                " 'format' = 'json', " +
                "'json.timestamp-format.standard' = 'ISO-8601'"+
                ")";
        // Define Kafka Sink Table
        String createSinkTable = "CREATE TABLE kafka_sink (" +
                " id INT, " +
                " name STRING " +
                ") WITH (" +
                " 'connector' = 'kafka', " +
                " 'topic' = 'output_topic', " +                 // //for receiving the message the messages
                " 'properties.bootstrap.servers' = 'niit:9092', " +
                " 'format' = 'json' " +
                ")";
        tableEnv.executeSql(createSourceTable);
        tableEnv.executeSql(createSinkTable);
        // Triggers execution by itself
        TableResult result= tableEnv.executeSql("INSERT INTO kafka_sink SELECT id, name FROM kafka_source WHERE id > 100");
        result.getJobClient().get().getJobExecutionResult().get();
    }
}