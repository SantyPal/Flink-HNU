package com.niit.ch5;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class SocketCEP_Debug {

    public static class LoginEvent {
        public String user;
        public String status;

        public LoginEvent() {}

        public LoginEvent(String user, String status) {
            this.user = user;
            this.status = status;
        }

        @Override
        public String toString() {
            return user + "," + status;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 👂 Connect to socket
        DataStream<String> rawStream = env.socketTextStream("localhost", 9999);
        rawStream.print("📥 RAW");

        // 🧾 Parse raw input to LoginEvent
        SingleOutputStreamOperator<LoginEvent> events = rawStream
                .map(line -> {
                    try {
                        String[] parts = line.split(",");
                        String user = parts[0].trim();
                        String status = parts[1].trim();
                        System.out.println("✅ Parsed: " + user + "," + status);
                        return new LoginEvent(user, status);
                    } catch (Exception e) {
                        System.err.println("❌ Parse error: " + line);
                        return new LoginEvent("unknown", "fail");
                    }
                });

        // 🎯 CEP pattern: two consecutive fails by same user within 10 seconds
        Pattern<LoginEvent, ?> pattern = Pattern.<LoginEvent>begin("firstFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equalsIgnoreCase("fail");
                    }
                })
                .next("secondFail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent event) {
                        return event.status.equalsIgnoreCase("fail");
                    }
                })
                .within(Time.seconds(10));

        // 🧠 Apply pattern and react
        DataStream<String> result = CEP.pattern(events.keyBy(e -> e.user), pattern)
                .select(new PatternSelectFunction<LoginEvent, String>() {
                    @Override
                    public String select(Map<String, List<LoginEvent>> pattern) {
                        String user = pattern.get("firstFail").get(0).user;
                        String msg = "⚠️ Detected two failures for user: " + user;
                        System.out.println("🎯 CEP MATCH: " + msg);
                        return msg;
                    }
                });

        result.print("📣 CEP");

        env.execute("🔍 Flink CEP Socket Debug");
    }
}
