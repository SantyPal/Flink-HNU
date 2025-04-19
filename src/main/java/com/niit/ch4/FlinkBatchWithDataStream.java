package com.niit.ch4;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkBatchWithDataStream {
    public static void main(String[] args) throws Exception {
        // Set up environment in BATCH mode
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); // <-- Key for batch
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Create a DataStream
        DataStream<Person> dataStream = env.fromElements(
                new Person("Alice", 30),
                new Person("Bob", 25)
        );

        // Register as a table and query
        tableEnv.createTemporaryView("People", dataStream);
        Table resultTable = tableEnv.sqlQuery("SELECT name, age FROM People WHERE age >= 30");

        // Print results
        tableEnv.toDataStream(resultTable, Row.class).print();
        env.execute();
    }

    public static class Person {
        public String name;
        public int age;

        public Person() {} // Required for Flink

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }
    }
}
