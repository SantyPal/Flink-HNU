package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BatchFlatMap {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> sentences = env.fromElements("Apache Flink", "Batch Processing");
// FlatMap transformation to split sentences into words
        DataSet<String> words = sentences.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String sentence, Collector<String> out) {
                for (String word : sentence.split(" ")) {
                    out.collect(word);
                }
            }
        });
        words.print();
    }
}
