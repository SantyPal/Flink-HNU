package com.niit.ch3.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WCFile {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //The path of the file, as a URI
        //(e.g., "file:///some/local/file" or "hdfs://host:port/file/path").
        DataSet<String> text = env.readTextFile("src/main/resources/wcfile.txt");

        DataSet<Tuple2<String, Integer>> wordCounts = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String word : line.split(" ")) {
                            out.collect(new Tuple2<String, Integer>(word, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1);
        wordCounts.print();
    }
}
