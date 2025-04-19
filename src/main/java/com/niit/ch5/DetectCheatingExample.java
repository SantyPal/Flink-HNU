package com.niit.ch5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DetectCheatingExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Sample student events
        DataStream<StudentEvent> input = env.fromElements(
                new StudentEvent("Alice", "start_exam", 100L),
                new StudentEvent("Bob", "start_exam", 105L),
                new StudentEvent("Alice", "submit_exam", 102L),  // Detected: too fast
                new StudentEvent("Bob", "submit_exam", 110L)     // Not detected
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<StudentEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((event, ts) -> event.timestamp * 1000) // milliseconds
        );

        // Define the pattern: start_exam followed by submit_exam within 3 seconds
        Pattern<StudentEvent, ?> cheatingPattern = Pattern.<StudentEvent>begin("start")
                .where(new IterativeCondition<StudentEvent>() {
                    @Override
                    public boolean filter(StudentEvent e, Context<StudentEvent> ctx) {
                        return e.eventType.equals("start_exam");
                    }
                })
                .next("submit")
                .where(new IterativeCondition<StudentEvent>() {
                    @Override
                    public boolean filter(StudentEvent e, Context<StudentEvent> ctx) {
                        return e.eventType.equals("submit_exam");
                    }
                })
                .within(Time.seconds(3));
//
        // Apply pattern per student
        PatternStream<StudentEvent> patternStream = CEP.pattern(
                input.keyBy(e -> e.name),
                cheatingPattern
        );

        // Select and print the matches
        SingleOutputStreamOperator<String> result = patternStream.select(
                (PatternSelectFunction<StudentEvent, String>) pattern -> {
                    StudentEvent start = pattern.get("start").get(0);
                    StudentEvent submit = pattern.get("submit").get(0);
                    long currentTime = System.currentTimeMillis();

                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    String formattedTime = sdf.format(new Date(currentTime));

                    // Return warning with human-readable system time
                    return "Warning: " + start.name + " may have submitted the exam too quickly! at " + formattedTime;
                }
        );

        result.print();
        env.execute("Detect Cheating in Online Exam");
    }

    // Public fields version of the event class
    public static class StudentEvent {
        public String name;
        public String eventType;
        public long timestamp;

        public StudentEvent() {}

        public StudentEvent(String name, String eventType, long timestamp) {
            this.name = name;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return name + " - " + eventType + " @ " + timestamp;
        }
    }
}
