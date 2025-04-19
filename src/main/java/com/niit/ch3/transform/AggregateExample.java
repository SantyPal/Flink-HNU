package com.niit.ch3.transform;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class AggregateExample {
    public static void main(String[] args) throws Exception {
        // Set up Flink execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample dataset (category, value)
        DataSet<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("A", 10), Tuple2.of("B", 5), Tuple2.of("A", 20), Tuple2.of("B", 15), Tuple2.of("C", 25));

        // Perform sum aggregation (total value per category)
        DataSet<Tuple2<String, Integer>> sumResult = input.groupBy(0).sum(1);       //GroupCombine with sum

        // Perform min aggregation (minimum value per category)
        DataSet<Tuple2<String, Integer>> minResult = input.groupBy(0).min(1);

        // Perform max aggregation (maximum value per category)
        DataSet<Tuple2<String, Integer>> maxResult = input.groupBy(0).max(1);

        // Print results separately
        System.out.println("Sum Aggregation:");
        sumResult.print();

        System.out.println("Min Aggregation:");
        minResult.print();

        System.out.println("Max Aggregation:");
        maxResult.print();

        /*Dataset Processing

"A" appears twice (10 and 20), so sum = 30, min = 10, max = 20.
"B" appears twice (5 and 15), so sum = 20, min = 5, max = 15.
"C" appears once (25), so all aggregate values are 25.*/
    }
}
