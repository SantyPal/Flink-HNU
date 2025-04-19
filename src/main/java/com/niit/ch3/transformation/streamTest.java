package com.niit.ch3.transformation;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class streamTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(1, 2, 3, 4, 5) // Data Source
                .map(i -> 2 * i) // Operator
                .print(); // Sink

        env.execute();
    }
}
