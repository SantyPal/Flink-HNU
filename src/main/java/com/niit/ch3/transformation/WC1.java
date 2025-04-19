package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WC1 {
    public static void main(String[] args) throws Exception {
        //step = set the environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //load the data
        DataSet<String> text = env.fromCollection(Arrays.asList("This is line one. This is my line number 2. Third line is here"
                .split(" "))); //slice the data based on the delimiter

        //4)
        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {//read all lines and convert into single words to put intuple
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : line.split(" ")) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .groupBy(0)                         //StreamProcessing - keyBy
                .sum(1);
        wordCounts.print();
    }
}
