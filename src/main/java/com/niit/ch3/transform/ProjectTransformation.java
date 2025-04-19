package com.niit.ch3.transform;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.List;

public class ProjectTransformation {

    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create a sample dataset
        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                Tuple3.of(1, "Alice", 99.5),        //f0=1, f1=alice, f2=99.5
                Tuple3.of(2, "Bob", 87.0),
                Tuple3.of(3, "Charlie", 92.3)
        );

        // Apply project transformation (selecting only name and ID, reordering fields)
        DataSet<Tuple2<String, Integer>> output = input.project(1, 0);

        // Print the results
        output.print();
    }
}