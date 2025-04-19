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
public class LoginFailureDetection {
    // Simple POJO
    public static class LoginEvent {
        public String user;         //userID
        public String status;       //login success or fail
        public long timestamp;      //at what time

        public LoginEvent() {}

        public LoginEvent(String user, String status, long timestamp) {
            this.user = user;
            this.status = status;
            this.timestamp = timestamp;
        }

        public String toString() {
            return user + " " + status + " " + timestamp;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Simulated input stream
        DataStream<LoginEvent> inputEventStream = env.fromElements(
                new LoginEvent("Alice", "badword", 1000L),         //time in unix system and in seconds
                new LoginEvent("Alice", "fail", 2000L),
                new LoginEvent("Bob", "success", 3000L),
                new LoginEvent("Alice", "success", 4000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<LoginEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.timestamp)
        );
        // CEP Pattern: 2 consecutive "fail"
        Pattern<LoginEvent, ?> loginFailPattern = Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("badword");
                    }
                })
                .next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equals("fail");
                    }
                })      //fail is in my datastream
                .within(Time.seconds(10));

        // Apply pattern
        DataStream<String> result = CEP.pattern(inputEventStream.keyBy(e -> e.user), loginFailPattern)
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) {
                        LoginEvent first = pattern.get("firstFail").get(0);
                        LoginEvent second = pattern.get("secondFail").get(0);
                        return "Detected consecutive failures for user: " + first.user+second.user;
                    }
                });

        result.print();
        env.execute("Login Failure Pattern");
    }
}

