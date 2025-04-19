package com.niit.ch4.book;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BatchJob {
    public static void main(String[] args) throws Exception {
        // Initialize StreamExecutionEnvironment and TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Register the CSV source as a table
        tableEnv.createTemporaryTable(
                "athletes",
                TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder()
                                .column("country", DataTypes.STRING())
                                .column("total", DataTypes.INT())
                                .build())
                        .format("csv")
                        .option("path", "file:///D:\\NIIT\\TIRM-2021\\Flink-En\\EN\\FlinkProgramming-solution\\src\\main\\resources\\ch4_data\\olympic-athletes.csv") // Ensure the path is correct
                        .option("csv.field-delimiter", ",")
                        .option("csv.ignore-parse-errors", "true")
                        .option("csv.charset", "UTF-8") // Ensure correct charset
                        .build()
        );

        // Execute SQL query
      //  TableResult result = tableEnv.executeSql("SELECT country, SUM(total) AS sum_total FROM athletes GROUP BY country");
        TableResult result = tableEnv.executeSql("SELECT * FROM athletes");
        // Print results
       // result.collect().forEachRemaining(System.out::println);
        result.print();
        env.execute();
    }
}
