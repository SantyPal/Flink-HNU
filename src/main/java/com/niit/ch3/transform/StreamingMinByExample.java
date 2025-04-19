package com.niit.ch3.transform;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingMinByExample {
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sample project cost stream: (Category, Cost)
        DataStream<Tuple2<String, Double>> projectStream = env.fromElements(
                Tuple2.of("AI", 1000.0),
                Tuple2.of("Cloud", 2000.0),
                Tuple2.of("AI", 1500.0),
                Tuple2.of("Cloud", 1800.0),
                Tuple2.of("AI", 900.0)  // AI category gets a lower cost update
        );

        // Key by category and find the project with the minimum cost dynamically
        KeyedStream<Tuple2<String, Double>, String> keyedStream = projectStream.keyBy(value -> value.f0);
        DataStream<Tuple2<String, Double>> minCostStream = keyedStream.minBy(1);

        // Print the results
        minCostStream.print();

        // Execute the streaming job
        env.execute("Real-Time MinBy Project Cost");
    }
}