package com.niit.ch4.Operators;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkUnionExample {
    public static void main(String[] args) throws Exception {
        // 1. Create ExecutionEnvironment (Batch processing)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. Create two DataSets
        DataSet<String> dataset1 = env.fromElements("Alice", "Bob", "Charlie","Eve");
        DataSet<String> dataset2 = env.fromElements("David", "Eve", "Frank");

        // 3. Use union() to combine both DataSets
        DataSet<String> result = dataset1.union(dataset2);

        // 4. Print the result
        result.print();
    }
}