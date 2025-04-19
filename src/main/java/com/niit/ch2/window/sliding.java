package com.niit.ch2.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class sliding {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Constructing input data
        List<Tuple2<String, Long>> data = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        data.add(a);
        data.add(b);
        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(data);

        // Use ProcessTime sliding window, 10s as a window length, sliding every 1s
        input.keyBy(x -> x.f1)
                .timeWindow(Time.seconds(30), Time.seconds(10))
                .reduce(new MyWindowFunction());
input.print();
        env.execute();
    }

    public static class MyWindowFunction implements ReduceFunction<Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
            return new Tuple2<>(t1.f0 + t2.f0, t1.f1);
        }
    }
}
