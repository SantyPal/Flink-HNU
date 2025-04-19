package com.niit.ch3.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class DistinctExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample dataset with duplicates (category, value)
        DataSet<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("A", 10),         //0
                Tuple2.of("B", 5),          //1
                Tuple2.of("A", 10),  // Duplicate   //2
                Tuple2.of("B", 15),         //3
                Tuple2.of("C", 25),         //4
                Tuple2.of("C", 25)   // Duplicate   //5
        );

        // Apply distinct transformation to remove duplicates
        DataSet<Tuple2<String, Integer>> distinctResult = input.distinct();

        // Print the distinct results
        System.out.println("Distinct Transformation Result:");
        distinctResult.print();
    }
}
