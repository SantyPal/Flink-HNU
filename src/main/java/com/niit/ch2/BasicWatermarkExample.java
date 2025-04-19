package com.niit.ch2;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class BasicWatermarkExample {
    public static void main(String[] args) throws Exception {
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulated data stream with timestamps
        DataStream<String> input = env.fromElements(
                "event1,1700000000000",  // Event with timestamp
                "event2,1700000005000",
                "event3,1700000010000"
        );

        // Assign timestamps and watermarks
        DataStream<String> withWatermarks = input
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forMonotonousTimestamps() //Assumes events arrive in order with no late arrivals.
                );

        // Print the stream
        withWatermarks.print();

        // Execute the Flink job
        env.execute("Basic Watermark Strategy Example");
    }
}
