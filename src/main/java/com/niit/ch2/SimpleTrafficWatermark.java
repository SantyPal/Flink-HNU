package com.niit.ch2;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

// Define a simple Car Event
class CarEvent {
    public String carId;
    public long timestamp; // Event time

    public CarEvent(String carId, long timestamp) {
        this.carId = carId;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Car " + carId + " passed at " + timestamp;
    }
}

public class SimpleTrafficWatermark {
    public static void main(String[] args) throws Exception {
        // 1️⃣ Set up Flink environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2️⃣ Create a stream of Car Events (Some events are late)
        DataStream<CarEvent> carStream = env.fromElements( //Socket
                new CarEvent("A", System.currentTimeMillis() - 7000), // 7 sec ago (late)
                new CarEvent("B", System.currentTimeMillis() - 2000), // 2 sec ago
                new CarEvent("C", System.currentTimeMillis() - 5000)  // 5 sec ago
        );

//        Date pastDate1 = new Date(System.currentTimeMillis() - 7000);
//        Date pastDate2 = new Date(System.currentTimeMillis() - 2000);
//        Date pastDate3 = new Date(System.currentTimeMillis() - 5000);
////        // Format date to human-readable string
//        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
//        String formattedTime = sdf.format(pastDate1);
//        String formattedTime1 = sdf.format(pastDate2);
//        String formattedTime2 = sdf.format(pastDate3);
//////
//        System.out.println("Time 7 seconds ago: " + formattedTime + ":" + pastDate1);
//        System.out.println("Time 2 seconds ago: " + formattedTime1+ ":" + pastDate2);
//        System.out.println("Time 5 seconds ago: " + formattedTime2+ ":" + pastDate3);

        // 3️⃣ Assign timestamps & watermarks (Allowing 3 sec lateness)
        DataStream<CarEvent> watermarkedStream = carStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<CarEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.timestamp)
        );

        // 4️⃣ Process the stream (Just printing events)
        watermarkedStream.print();

        // 5️⃣ Start execution
        env.execute("Basic Flink Watermark Example");
    }
}