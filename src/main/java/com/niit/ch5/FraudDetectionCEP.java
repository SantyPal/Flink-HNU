package com.niit.ch5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class FraudDetectionCEP {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Generate a stream of transaction events
        DataStream<Transaction> transactionStream = env
                .fromElements(
                        new Transaction("user1", 1000, 1L),
                        new Transaction("user2", 7000, 2L), // High-value transaction
                        new Transaction("user2", 8000, 6L), // Another high-value transaction (Suspicious) - i should see this
                        new Transaction("user3", 300, 8L),
                        new Transaction("user2", 9000, 12L), // Third high-value transaction
                        new Transaction("user1", 6000, 15L), // High-value transaction
                        new Transaction("user1", 7000, 18L)  // Another high-value transaction (Suspicious) - should see this
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        // Define a pattern for fraud detection
        Pattern<Transaction, ?> fraudPattern = Pattern.<Transaction>begin("firstHighValue")
                .where(SimpleCondition.of(tx -> tx.getAmount() > 5000))  // First high-value transaction - for condition simpleCondition class
                .next("secondHighValue")
                .where(SimpleCondition.of(tx -> tx.getAmount() > 5000))  // Second high-value transaction
                .within(Time.seconds(10));  // Must occur within 10 seconds

        //// Apply the pattern to the transaction stream
        PatternStream<Transaction> patternStream = CEP.pattern(transactionStream, fraudPattern);
        // Select and process suspicious transactions
        DataStream<String> fraudAlerts = patternStream.select((Map<String, List<Transaction>> patternMatch) -> {
            Transaction first = patternMatch.get("firstHighValue").get(0);
            Transaction second = patternMatch.get("secondHighValue").get(0);
            return "🚨 Fraud Alert! User " + first.getUserId() + " made two high-value transactions: "
                    + first + " -> " + second;
        });
        // Print fraud alerts
        fraudAlerts.print();

        // Execute the Flink job
        env.execute("Flink CEP Fraud Detection Example");
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
