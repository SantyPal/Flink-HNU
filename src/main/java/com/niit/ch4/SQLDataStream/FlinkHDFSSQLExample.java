package com.niit.ch4.SQLDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.configuration.Configuration;

public class FlinkHDFSSQLExample {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setString("classloader.check-leaked-classloader", "false");
        // Set up environments
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define HDFS Source Table buffer memory (Table Environment)
        String createHdfsTable = "CREATE TABLE hdfs_table (" +
                " user_id INT, " +
                " action STRING, " +
                " `timestamp` TIMESTAMP(3) " + // escape reserved keyword
                ") WITH (" +
                " 'connector' = 'filesystem', " +       //for hdfs files to convert into table - need to add the depedency
                " 'path' = 'hdfs://niit:9000/data/input.json', " +  //hdfs path of json data
                " 'format' = 'json', " +
                " 'json.timestamp-format.standard' = 'ISO-8601' " + // this helps with timestamp parsing
                ")";
        // Define HDFS Sink Table in buffer memory
        String createHdfsSinkTable = "CREATE TABLE hdfs_output (" +
                " user_id INT, " +
                " action STRING " +
                ") WITH (" +
                " 'connector' = 'filesystem', " +
                " 'path' = 'hdfs://niit:9000/output/results.json', " +  //hdfs path to store the output
                " 'format' = 'json' " +
                ")";
        // Register tables
        tableEnv.executeSql(createHdfsTable);
        tableEnv.executeSql(createHdfsSinkTable);
        // Insert query
        tableEnv.executeSql("INSERT INTO hdfs_output SELECT user_id, action FROM hdfs_table WHERE action = 'click'");
        // No need to call env.execute() for SQL-only jobs in Table API
    }
}