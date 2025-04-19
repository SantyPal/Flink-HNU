package com.niit.ch4.Operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkIntersectAllExample {
    public static void main(String[] args) throws Exception {
        // 1. Create ExecutionEnvironment (Batch processing)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. Create two DataSets with duplicates
        DataSet<String> dataset1 = env.fromElements("Alice", "Bob", "Charlie", "Bob", "David");
        DataSet<String> dataset2 = env.fromElements("Bob", "Charlie", "Eve", "Bob");

        // 3. Perform an inner join to simulate INTERSECT ALL (with duplicates)
        DataSet<String> intersectAll = dataset1
                .join(dataset2)  // Perform join between the two datasets
                .where(value -> value)  // Match on the value of the dataset
                .equalTo(value -> value)  // Matching on the value of the dataset
                .with((first, second) -> first);  // Return the first dataset's value (simulates intersection)

        // 4. Print the result
        intersectAll.print();
    }
}
