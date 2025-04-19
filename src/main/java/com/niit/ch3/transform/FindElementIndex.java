package com.niit.ch3.transform;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class FindElementIndex {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample dataset with Tuple2 values
        DataSet<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("A", 10),
                Tuple2.of("B", 5),
                Tuple2.of("A", 10),  // Duplicate
                Tuple2.of("B", 15),
                Tuple2.of("C", 25),
                Tuple2.of("C", 25)   // Duplicate
        );
        // Assign an index to each element
        DataSet<Tuple2<Integer, Tuple2<String, Integer>>> indexedData = input.mapPartition(new IndexAssigner());

        // Print the indexed dataset
        indexedData.print();
    }
    // Custom function to assign an index to each element
    public static class IndexAssigner extends RichMapPartitionFunction<Tuple2<String, Integer>, Tuple2<Integer, Tuple2<String, Integer>>> {
        @Override
        public void mapPartition(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<Integer, Tuple2<String, Integer>>> out) {
            int index = 0;
            for (Tuple2<String, Integer> value : values) {
                out.collect(new Tuple2<>(index++, value));
            }
        }
    }
}