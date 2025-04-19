package com.niit.ch4.Operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class FlinkIntersectExample {
    public static void main(String[] args) throws Exception {
        // 1. Create ExecutionEnvironment (Batch processing)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. Create two DataSets
        DataSet<String> dataset1 = env.fromElements("Alice", "Bob", "Charlie", "David");
        DataSet<String> dataset2 = env.fromElements("Bob", "Charlie", "Eve");

        // 3. Perform an inner join to simulate intersection
        DataSet<String> intersection = dataset1
                .join(dataset2)
                .where(value -> value)  // This is the field we're matching on
                .equalTo(value -> value)  // Matching on the value of the dataset
                .with((first, second) -> first); // Return the first dataset's value (simulates intersection)

        // 4. Print the result
        intersection.print();
    }
}
