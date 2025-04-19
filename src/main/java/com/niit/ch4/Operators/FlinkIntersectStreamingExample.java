package com.niit.ch4.Operators;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkIntersectStreamingExample {
    public static void main(String[] args) throws Exception {
        // 1. Create StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. Create TableEnvironment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3. Create two stream tables (unbounded)
        Table dataset1 = tableEnv.fromDataStream(
                env.fromElements(
                        new Person("Alice"),
                        new Person("Bob"),
                        new Person("Charlie")
                ),
                $("name") // Define schema
        );

        Table dataset2 = tableEnv.fromDataStream(
                env.fromElements(
                        new Person("Bob"),
                        new Person("Charlie"),
                        new Person("David")
                ),
                $("name") // Define schema
        );

        // 4. Perform inner join to simulate intersection
        Table intersection = dataset1
                .join(dataset2)
                .where($("name").isEqual($("name")))
                .select($("name"));

        // 5. Execute the query and print results
        intersection.execute().print();
    }

    // POJO class
    public static class Person {
        public String name;

        public Person() {}

        public Person(String name) {
            this.name = name;
        }
    }
}
