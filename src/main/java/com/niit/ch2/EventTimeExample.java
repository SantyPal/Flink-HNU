package com.niit.ch2;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;

public class EventTimeExample {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable event time processing
        env.setParallelism(1);

        // Create a data stream from a custom source
        DataStream<Event> eventStream = env.addSource(new EventSource());

        // Assign timestamps and watermarks
        SingleOutputStreamOperator<Event> withTimestampsAndWatermarks = eventStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0)) // Allow up to 5 seconds lateness
                                .withTimestampAssigner((event, timestamp) -> event.timestamp) // Extract event time
                );

        // Print the processed stream
        withTimestampsAndWatermarks.print();

        // Execute the Flink job
        env.execute("Basic Event Time Processing");
    }

    // Event class representing incoming data
    public static class Event {
        public final String name;
        public final long timestamp;

        public Event(String name, long timestamp) {
            this.name = name;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Event{name='" + name + "', timestamp=" + timestamp + "}";
        }
    }

    // Custom Source Function to generate events with timestamps
    public static class EventSource implements SourceFunction<Event> {
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            ctx.collect(new Event("event1", System.currentTimeMillis()));
            Thread.sleep(1000);
            ctx.collect(new Event("event2", System.currentTimeMillis() - 4000)); // Slightly late event
            Thread.sleep(1000);
            ctx.collect(new Event("event3", System.currentTimeMillis() - 7000)); // Late event
        }

        @Override
        public void cancel() {}
    }
}