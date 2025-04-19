package com.niit.ch2.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class global2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.socketTextStream("niit", 9900);

        DataStream<Tuple2<String, Integer>> globalCounts = input
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                })
                .keyBy(value -> value.f0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(10)) // explicit trigger that sums every 100 records
                .sum(1);
        globalCounts.print();       //1. sink - stdout
        globalCounts.writeAsText("src/main/resources/output.txt");
        env.execute();
    }

}
