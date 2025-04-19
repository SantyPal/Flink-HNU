package com.niit.all_wc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Random;

public class FlinkWindowWordCount {

    public static void main(String[] args) throws Exception {
        // Create a streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Create a simulated data source that generates one random word per second
        DataStream<String> textStream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;
            private final String[] words = {"flink", "window", "streaming", "processing", "real-time"};

            @Override
            public void run(SourceContext<String> ctx) throws InterruptedException {
                Random random = new Random();
                while (isRunning) {
                    String word = words[random.nextInt(words.length)];
                    ctx.collect(word);
                    Thread.sleep(1000);  // Generate a word every second
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        // Group and count streaming data
        DataStream<Tuple2<String, Integer>> wordCounts = textStream
                .assignTimestampsAndWatermarks( WatermarkStrategy.noWatermarks())
                .map(word -> new Tuple2<>(word, 1))  // Map each word to a binary of (word, 1)
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)  // Group by word (i.e., the first element of the tuple)
                .timeWindow(Time.seconds(5))  // set the time window with a size of 5 seconds
                .sum(1);  // Count the words in each window

        // Print the results
        wordCounts.print();

        // Execute the program
        env.execute("Flink Window Word Count");
    }
}

