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

public class SocketCEP {

    public static class LoginEvent {
        public String user;
        public String status;

        public LoginEvent() {}

        public LoginEvent(String user, String status) {
            this.user = user;
            this.status = status;
        }

        public String toString() {
            return user + "," + status;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1️⃣ Read from socket
        DataStream<String> rawStream = env.socketTextStream("localhost", 9999);
        rawStream.print("📥 RAW");

        // 2️⃣ Map to LoginEvent
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

        // 3️⃣ Define CEP pattern
        Pattern<LoginEvent, ?> pattern = Pattern.<LoginEvent>begin("firstFail")
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
                .within(Time.seconds(10));

        // 4️⃣ Apply pattern
        events.map(e -> "✅ Received event: " + e).print("📣 EVENTS");
//                .select(new PatternSelectFunction<LoginEvent, String>() {
//                    @Override
//                    public String select(Map<String, List<LoginEvent>> pattern) {
//                        LoginEvent first = pattern.get("firstFail").get(0);
//                        LoginEvent second = pattern.get("secondFail").get(0);
//                        String msg = "⚠️ Detected two failures for user: " + first.user;
//                        System.out.println("🎯 CEP MATCH: " + msg);
//                        return msg;
//                    }
//                });

        //result.print("📣 CEP");

        env.execute("Flink CEP with Socket Input");
    }
}
