package com.niit.ch5;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.PatternStream;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class SessionTimeoutExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> input = env.fromElements(
                new UserEvent("Alice", "page_view", 1000L),
                new UserEvent("Alice", "interaction", 4000L),
                new UserEvent("Alice", "page_view", 7000L),
                new UserEvent("Bob", "page_view", 1500L),
                new UserEvent("Bob", "interaction", 3500L),
                new UserEvent("Alice", "page_view", 9000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.timestamp)
        );

        // Define CEP pattern to detect inactivity (timeout window = 5 minutes)
        Pattern<UserEvent, ?> sessionPattern = Pattern.<UserEvent>begin("activity")
                .where(new IterativeCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event, Context<UserEvent> ctx) {
                        return event.eventType.equals("page_view") || event.eventType.equals("interaction");
                    }
                })
                .notFollowedBy("next")
                .where(new IterativeCondition<UserEvent>() {
                    @Override
                    public boolean filter(UserEvent event, Context<UserEvent> ctx) {
                        return true; // Catch anything after "activity"
                    }
                })
                .within(Time.minutes(5));

        PatternStream<UserEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.username),
                sessionPattern
        );
//
        SingleOutputStreamOperator<String> result = patternStream.select(
                new PatternSelectFunction<UserEvent, String>() {
                    @Override
                    public String select(Map<String, List<UserEvent>> pattern) {
                        UserEvent activity = pattern.get("activity").get(0);
                        return "User " + activity.username + " session has timed out.";
                    }
                }
        );

        result.print();

        env.execute("Session Timeout Detection - Flink 1.17");
    }

    // UserEvent class with public fields
    public static class UserEvent {
        public String username;
        public String eventType;
        public long timestamp;

        public UserEvent() {}

        public UserEvent(String username, String eventType, long timestamp) {
            this.username = username;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return username + " - " + eventType + " @ " + timestamp;
        }
    }
}
