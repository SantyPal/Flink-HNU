package com.niit.ch3.transform;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.operators.AggregateOperator;

import java.util.Arrays;
import java.util.List;

public class ProjectAggregation {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample project data: (Project ID, Category, Cost)
        List<Tuple3<Integer, String, Double>> projects = Arrays.asList(
                Tuple3.of(1, "AI", 1000.0),
                Tuple3.of(2, "Cloud", 2000.0),
                Tuple3.of(3, "AI", 1500.0),
                Tuple3.of(4, "Cloud", 3000.0),
                Tuple3.of(5, "AI", 1200.0)
        );

        // Create a Flink dataset from the collection
        DataSet<Tuple3<Integer, String, Double>> projectDataSet = env.fromCollection(projects);

        // Aggregate: Sum the costs for each project category
        AggregateOperator<Tuple3<Integer, String, Double>> aggregatedCosts =
                projectDataSet
                        .groupBy(1) // Group by category (2nd field)
                        .sum(2); // Sum the costs (3rd field)

        // Print the aggregated results
        aggregatedCosts.print();
    }
}