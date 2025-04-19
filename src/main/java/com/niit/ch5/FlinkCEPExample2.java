package com.niit.ch5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FlinkCEPExample2 {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Create a DataStream of events
        DataStream<Event> inputEvents = env
                .fromElements(
                        new Event("start", 1L), //1L+6 - will it be used?

                        new Event("random", 2L),
                        new Event("middle", 3L),
                        new Event("end", 4L),
                        new Event("start", 5L),
                        new Event("middle", 6L),
                        new Event("end", 7L)
                )
                .assignTimestampsAndWatermarks(
                       // WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
//
        /* Define the pattern: "start" -> "middle" -> "end"
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> "start".equals(event.getName())))
                .next("middle")
                .where(SimpleCondition.of(event -> "middle".equals(event.getName())))
                .next("end")
                .where(SimpleCondition.of(event -> "end".equals(event.getName())));*/

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
                .where(SimpleCondition.of(event -> "start".equals(event.getName())))
                .followedBy("middle")  // Allow non-matching events in between
                .where(SimpleCondition.of(event -> "middle".equals(event.getName())))
                .followedBy("end")
                .where(SimpleCondition.of(event -> "end".equals(event.getName())));

        // Apply the pattern to the event stream
        PatternStream<Event> patternStream = CEP.pattern(inputEvents, pattern);

        // Select and process the matched events
        SingleOutputStreamOperator<String> resultStream = patternStream.select(
                (Map<String, List<Event>> patternMatch) -> {
                    Event start = patternMatch.get("start").get(0);
                    Event middle = patternMatch.get("middle").get(0);
                    Event end = patternMatch.get("end").get(0);
                    return "Pattern matched: " + start + " -> " + middle + " -> " + end;
                }
        );

        // Print the output
        resultStream.print();

        // Execute the Flink job
        env.execute("Flink CEP Basic Example");
    }

    // Event class
    public static class Event {
        private String name;
        private long timestamp;

        public Event() {}

        public Event(String name, long timestamp) {
            this.name = name;
            this.timestamp = timestamp;
        }

        public String getName() {
            return name;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Event{name='" + name + "', timestamp=" + timestamp + "}";
        }
    }
}
