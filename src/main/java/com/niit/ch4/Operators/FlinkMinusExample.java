package com.niit.ch4.Operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;

public class FlinkMinusExample {
    public static void main(String[] args) throws Exception {
        // 1. Create ExecutionEnvironment (Batch processing)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. Create two DataSets
        DataSet<String> dataset1 = env.fromElements("Alice", "Bob", "Charlie", "David");
        DataSet<String> dataset2 = env.fromElements("Bob", "Charlie", "Eve");

        // 3. Collect dataset2 into a List (to use contains in filter)
        List<String> dataset2List = dataset2.collect();

        // 4. Perform the "MINUS" operation using filter
        DataSet<String> result = dataset1.filter(value -> !dataset2List.contains(value));

        // 5. Print the result
        result.print();
    }
}

