package com.niit.ch3.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class UnionExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // First dataset (Group A)
        DataSet<Tuple2<Integer, String>> dataset1 = env.fromElements(
                Tuple2.of(1, "Alice"),
                Tuple2.of(2, "Bob")
        );

        // Second dataset (Group B)
        DataSet<Tuple2<Integer, String>> dataset2 = env.fromElements(
                Tuple2.of(3, "Charlie"),
                Tuple2.of(2, "Bob") // Duplicate
        );

        // Perform Union transformation
        DataSet<Tuple2<Integer, String>> unionResult = dataset1.union(dataset2);

        // Print the result
        System.out.println("Union Transformation Result:");
        unionResult.print();
    }
}
