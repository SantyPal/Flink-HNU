package com.niit.ch3.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;


public class FirstNExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create a dataset
        DataSet<Tuple2<Integer, String>> dataset = env.fromElements(
                Tuple2.of(1, "Alice"),
                Tuple2.of(2, "Bob"),
                Tuple2.of(3, "Charlie"),
                Tuple2.of(4, "David"),
                Tuple2.of(5, "Eve")
        );

        // Apply first-n transformation (get first 3 elements)
        DataSet<Tuple2<Integer, String>> firstN = dataset.first(2);

        // Print the result
        firstN.print();
    }
}
