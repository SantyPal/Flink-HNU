package com.niit.ch5;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailureDetectionProcessingTime {

    // Simple POJO
    public static class LoginEvent {
        public String user;
        public String status;

        public LoginEvent() {}

        public LoginEvent(String user, String status) {
            this.user = user;
            this.status = status;
        }

        public String toString() {
            return user + " " + status;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulated input stream using processing time
        DataStream<LoginEvent> inputEventStream = env.fromElements(
                new LoginEvent("Alice", "fail"),
                new LoginEvent("Alice", "fail"),
                new LoginEvent("Bob", "success"),
                new LoginEvent("Alice", "success")
        );

        // CEP Pattern: 2 consecutive "fail"
        Pattern<LoginEvent, ?> loginFailPattern = Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("fail");
                    }
                })
                .next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("fail");
                    }
                })
                .within(Time.seconds(10)); // Uses processing time

        //// Apply pattern (with keyBy for user)
        DataStream<String> result = CEP.pattern(inputEventStream.keyBy(e -> e.user), loginFailPattern)
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) {
                        LoginEvent first = pattern.get("firstFail").get(0);
                        LoginEvent second = pattern.get("secondFail").get(0);
                        return "Detected consecutive failures for user: " + first.user;
                    }
                });

        result.print();
        env.execute("Login Failure Detection - Processing Time");
    }
}

