package com.niit.ch5;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

public class LoginFailureDetectionProcessingTime2 {

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

        // Simulated input stream using a custom source with delays
        DataStream<LoginEvent> inputEventStream = env.addSource(new SourceFunction<LoginEvent>() {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<LoginEvent> ctx) throws Exception {
                ctx.collect(new LoginEvent("Alice", "fail"));
                Thread.sleep(2000);
                ctx.collect(new LoginEvent("Alice", "fail"));
                Thread.sleep(2000);
                ctx.collect(new LoginEvent("Bob", "success"));
                Thread.sleep(1000);
                ctx.collect(new LoginEvent("Alice", "success"));
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        //// Define CEP pattern
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
                .within(Time.seconds(10)); // Processing time

        // Apply CEP pattern
        DataStream<LoginEvent> result = env.addSource(new SourceFunction<LoginEvent>() {
            private volatile boolean running = true;

            @Override
            public void run(SourceContext<LoginEvent> ctx) throws Exception {
                ctx.collect(new LoginEvent("Alice", "fail"));
                Thread.sleep(2000);
                ctx.collect(new LoginEvent("Alice", "fail"));
                Thread.sleep(2000);
                ctx.collect(new LoginEvent("Bob", "success"));
                Thread.sleep(1000);
                ctx.collect(new LoginEvent("Alice", "success"));
                Thread.sleep(5000); // 👈 Give Flink time to process pattern
            }

            @Override
            public void cancel() {
                running = false;
            }
        });


        result.print();
        env.execute("Login Failure Detection - Processing Time");
    }
}
