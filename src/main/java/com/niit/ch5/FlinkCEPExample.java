//package com.niit.ch5;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.cep.CEP;
//import org.apache.flink.cep.PatternSelectFunction;
//import org.apache.flink.cep.PatternStream;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.IterativeCondition;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import java.util.*;
//
//public class FlinkCEPExample {
//    // Simple event class with just user, type, and timestamp
//    public static class Event {
//        public String user;
//        public String type;
//        public long timestamp;
//
//        public Event(String user, String type, long timestamp) {
//            this.user = user;
//            this.type = type;
//            this.timestamp = timestamp;
//        }
//
//        public String getType() {
//            return type;
//        }
//
//        public String getUser() {
//            return user;
//        }
//
//        public long getTimestamp() {
//            return timestamp;
//        }
//
//        public String toString() {
//            return user + " - " + type + " - " + timestamp;
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        // 1. Create the Flink environment
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 2. Create a list of events based on your POJO Event class
//        List<Event> events = Arrays.asList(
//                new Event("Alice", "login", 1000),
//                new Event("Alice", "withdraw", 200000), // within 5 mins
//                new Event("Bob", "login", 3000),
//                new Event("Bob", "withdraw", 600000)    // too late
//        );
//
//        // 3. Create a stream from the event list and assign timestamps
//        DataStream<Event> stream = env.fromCollection(events)
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forMonotonousTimestamps()
//                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                );
//
//        // 4. Define a simple pattern: login followed by withdraw within 5 minutes
//        Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
//                .where(new IterativeCondition<Event>() {
//                    @Override
//                    public boolean filter(Event event, Context ctx) {
//                        return event.getType().equals("login");
//                    }
//                })
//                .next("end")
//                .where(new IterativeCondition<Event>() {
//                    @Override
//                    public boolean filter(Event event, Context ctx) {
//                        return event.getType().equals("withdraw");
//                    }
//                })
//                .within(Time.minutes(5));
//
//        // 5. Apply the pattern to the stream
//        PatternStream<Event> patternStream = CEP.pattern(stream, pattern);
//
//        // 6. When a match is found, format it as a simple string and print
//        patternStream.select(new PatternSelectFunction<Event, String>() {
//            @Override
//            public String select(Map<String, List<Event>> pattern) {
//                Event login = pattern.get("start").get(0);
//                Event withdraw = pattern.get("end").get(0);
//                return login.getUser() + " did login → withdraw within 5 minutes.";
//            }
//        }).print();
//
//        // 7. Execute the program
//        env.execute("Simple Flink CEP Example");
//    }
//}
