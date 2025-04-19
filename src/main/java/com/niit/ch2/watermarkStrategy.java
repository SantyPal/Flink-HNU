package com.niit.ch2;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class watermarkStrategy {
    public static void main(String[] args) throws Exception {
        // Set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // Use single parallelism for ordered output

        // Sample event data: "event_name,timestamp"
        DataStream<String> input = env.fromElements(
                "event1,1710000000000",  // Example timestamp in milliseconds
                "event2,1710000005000",
                "event3,1710000010000",
                "event4,1710000015000"
        );

        // Convert input data into Event POJO
        DataStream<Event> eventStream = input.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String value) {
                String[] parts = value.split(",");
                return new Event(parts[0], Long.parseLong(parts[1]));
            }
        });

        // Assign timestamps and watermarks
        SingleOutputStreamOperator<Event> timestampedStream = eventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // Allow 5s late events
                                .withTimestampAssigner((event, timestamp) -> event.timestamp)
                );

        // Apply windowed aggregation (count events in 10s event-time windows)
        timestampedStream
                .map(event -> 1) // Convert to numeric values for counting
                .returns(Integer.class)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum(0) // Sum event counts in each window
                .print();

        // Execute Flink job
        env.execute("Basic Watermark Strategy Example");
    }

    // POJO representing an Event
    public static class Event {
        public String name;
        public long timestamp;

        public Event() {} // Default constructor needed for Flink serialization

        public Event(String name, long timestamp) {
            this.name = name;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{name='" + name + "', timestamp=" + timestamp + "}";
        }
    }
}