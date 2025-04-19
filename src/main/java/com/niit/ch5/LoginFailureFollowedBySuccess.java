//package com.niit.ch5;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.cep.CEP;
//import org.apache.flink.cep.PatternSelectFunction;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.SimpleCondition;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.cep.PatternStream;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import java.time.Duration;
//
//class LoginEvent {
//    public String userId;
//    public String status; // "FAILED" or "SUCCESS"
//    public long timestamp;
//
//    public LoginEvent(String userId, String status, long timestamp) {
//        this.userId = userId;
//        this.status = status;
//        this.timestamp = timestamp;
//    }
//
//    public String getUserId() {
//        return userId;
//    }
//
//    public String getStatus() {
//        return status;
//    }
//
//    public long getTimestamp() {
//        return timestamp;
//    }
//
//   // @Override
//    public String toString() {
//        return "LoginEvent{" +
//                "userId='" + userId + '\'' +
//                ", status='" + status + '\'' +
//                ", timestamp=" + timestamp +
//                '}';
//    }
//}
//
//public class LoginFailureFollowedBySuccess {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        // Sample login event stream
//        DataStream<LoginEvent> loginEvents = env.fromElements(
//                new LoginEvent("user1", "FAILED", 1000L),
//                new LoginEvent("user1", "FAILED", 2000L),
//                new LoginEvent("user1", "SUCCESS", 250000L), // Within 5 minutes
//                new LoginEvent("user2", "FAILED", 4000L),
//                new LoginEvent("user2", "SUCCESS", 600000L) // More than 5 minutes, should not match
//        ).assignTimestampsAndWatermarks(
//                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//        );
//
//        // Define the pattern
//        Pattern<LoginEvent, ?> pattern = Pattern.<LoginEvent>begin("firstFail")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent event) {
//                        return event.getStatus().equals("FAILED");
//                    }
//                })
//                .next("secondFail")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent event) {
//                        return event.getStatus().equals("FAILED");
//                    }
//                })
//                .followedBy("success")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent event) {
//                        return event.getStatus().equals("SUCCESS");
//                    }
//                })
//                .within(Time.minutes(5));
//
//        // Apply the pattern
//        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEvents, pattern);
//
//        // Select matching patterns
//        SingleOutputStreamOperator<String> alerts = patternStream.select(
//                (PatternSelectFunction<LoginEvent, String>) patternMatch -> {
//                    LoginEvent firstFail = patternMatch.get("firstFail").get(0);
//                    LoginEvent secondFail = patternMatch.get("secondFail").get(0);
//                    LoginEvent success = patternMatch.get("success").get(0);
//                    return "ALERT: User " + firstFail.getUserId() +
//                            " had two failed logins followed by a success within 5 minutes!";
//                });
//
//        alerts.print();
//
//        env.execute("Detect Login Failures Followed by Success");
//    }
//}
