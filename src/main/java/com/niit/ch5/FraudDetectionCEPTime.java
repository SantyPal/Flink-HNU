package com.niit.ch5;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class FraudDetectionCEPTime {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate a stream of transaction events with timestamps and watermark
        DataStream<Transaction> transactionStream = env
                .fromElements(
                        new Transaction("user1", 1000, 1L), //stream will wait for 1+5
                        new Transaction("user2", 7000, 2L), //2+5
                        new Transaction("user2", 8000, 6L),
                        new Transaction("user3", 300, 8L),
                        new Transaction("user2", 9000, 12L),
                        new Transaction("user1", 6000, 15L),
                        new Transaction("user1", 7000, 18L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp() * 1000) // Convert to ms
                )
                .map(tx -> {
                    System.out.println("📥 Event received: " + tx + " | EventTime = " + Instant.ofEpochSecond(tx.getTimestamp()));
                    return tx;
                });

        // Define a pattern for detecting fraud
        Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("BadWord1")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.getAmount() > 5000;
                    }
                })
                .next("BadWord2")
                .where(new SimpleCondition<Transaction>() {
                    @Override
                    public boolean filter(Transaction tx) {
                        return tx.getAmount() > 5000;
                    }
                })
                .within(Time.seconds(10));

        // Apply the pattern to the transaction stream
        PatternStream<Transaction> patternStream = CEP.pattern(transactionStream, fraudPattern);

        // Select and process suspicious transactions
        DataStream<String> fraudAlerts = patternStream.select((Map<String, List<Transaction>> patternMatch) -> {
            Transaction first = patternMatch.get("BadWord1").get(0);
            Transaction second = patternMatch.get("BadWord2").get(0);
            return "🚨 Fraud Alert! User " + first.getUserId()
                    + " made two high-value transactions:\n   🔹 First: " + first
                    + " @ " + Instant.ofEpochSecond(first.getTimestamp())
                    + "\n   🔹 Second: " + second
                    + " @ " + Instant.ofEpochSecond(second.getTimestamp());
        });

        // Print fraud alerts
        fraudAlerts.print();

        // Execute the Flink job
        env.execute("Flink CEP Fraud Detection with Timestamps");
    }

    // Transaction class
    public static class Transaction {
        private String userId;
        private int amount;
        private long timestamp;

        public Transaction() {}

        public Transaction(String userId, int amount, long timestamp) {
            this.userId = userId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getUserId() {
            return userId;
        }

        public int getAmount() {
            return amount;
        }

        public long getTimestamp() {
            return timestamp;
        }

        @Override
        public String toString() {
            return "Transaction{user='" + userId + "', amount=" + amount + ", timestamp=" + timestamp + "}";
        }
    }
}
