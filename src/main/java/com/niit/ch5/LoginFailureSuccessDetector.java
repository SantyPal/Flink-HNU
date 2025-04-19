package com.niit.ch5;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Instant;
import java.util.List;
import java.util.Map;

class LoginEvent {
    private final String userId;
    private final long timestamp; // Event time in milliseconds
    private final String status; // "SUCCESS" or "FAILURE"

    public LoginEvent(String userId, long timestamp, String status) {
        this.userId = userId;
        this.timestamp = timestamp;
        this.status = status;
    }

    public String getUserId() {
        return userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getStatus() {
        return status;
    }

    @Override
    public String toString() {
        return "LoginEvent{userId='" + userId + "', timestamp=" + timestamp + ", status='" + status + "'}";
    }
}

public class LoginFailureSuccessDetector {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Enable event time processing
        env.setParallelism(1); // For simplicity in this example

        // Create a stream of login events
        DataStream<LoginEvent> loginEvents = env.addSource(new LoginEventSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
                    @Override
                    public long extractAscendingTimestamp(LoginEvent event) {
                        return event.getTimestamp();
                    }
                });

        // Define the pattern: Failure followed by Success within 5 minutes
        Pattern<LoginEvent, ?> pattern = Pattern.<LoginEvent>begin("failure")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getStatus().equals("FAILURE");
                    }
                })
                .followedBy("success")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.getStatus().equals("SUCCESS");
                    }
                })
                .within(Time.minutes(5));

        // Apply the pattern to the stream, grouped by userId
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginEvents.keyBy(LoginEvent::getUserId), pattern);

        // Process matched patterns and generate alerts
        DataStream<String> alerts = patternStream.select(
                (Map<String, List<LoginEvent>> matchedPattern) -> {
                    LoginEvent failureEvent = matchedPattern.get("failure").get(0);
                    LoginEvent successEvent = matchedPattern.get("success").get(0);
                    return String.format(
                            "Alert: User %s had a failed login at %d followed by a successful login at %d within 5 minutes.",
                            failureEvent.getUserId(), failureEvent.getTimestamp(), successEvent.getTimestamp()
                    );
                }
        );

        // Print the alerts
        alerts.print();

        // Execute the Flink job
        env.execute("Login Failure Followed by Success Detector");
    }

    // Custom source to simulate login events
    private static class LoginEventSource implements SourceFunction<LoginEvent> {
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<LoginEvent> ctx) throws Exception {
            // Simulate login events for a user
            String userId = "user123";
            long baseTime = Instant.now().toEpochMilli();

            // Event 1: Failure at base time
            ctx.collect(new LoginEvent(userId, baseTime, "FAILURE"));
            Thread.sleep(1000); // Wait 1 second

            // Event 2: Another failure 1 minute later
            ctx.collect(new LoginEvent(userId, baseTime + 60_000, "FAILURE"));
            Thread.sleep(1000);

            // Event 3: Success 2 minutes after base time (within 5 minutes)
            ctx.collect(new LoginEvent(userId, baseTime + 120_000, "SUCCESS"));
            Thread.sleep(1000);

            // Event 4: Failure 6 minutes after base time (outside window)
            ctx.collect(new LoginEvent(userId, baseTime + 360_000, "FAILURE"));
            Thread.sleep(1000);

            // Event 5: Success 7 minutes after base time (outside window for previous failure)
            ctx.collect(new LoginEvent(userId, baseTime + 420_000, "SUCCESS"));
        }
//
        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}