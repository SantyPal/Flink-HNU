package com.niit.ch6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SingleSourceShortestPaths;

public class SSSPExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create the edges representing friendships with weights (distance/effort between people)
        // Each edge is a relationship between two people with a weight representing distance/effort
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 1.0), // Alice is 1 unit away from Bob
                new Edge<>(2L, 3L, 1.0), // Bob is 1 unit away from Charlie
                new Edge<>(1L, 3L, 4.0), // Alice is 4 units away from Charlie (direct path)
                new Edge<>(3L, 4L, 2.0)  // Charlie is 2 units away from David
        );

        // Create a graph where each person (vertex) has an initial value (distance) of infinity
        Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, vertexId -> Double.POSITIVE_INFINITY, // All distances are initially set to infinity
                env);
        // Run the Single Source Shortest Path algorithm starting from Alice (1L) with max distance 10
        DataSet<Vertex<Long, Double>> result = graph
                .run(new SingleSourceShortestPaths<>(1L, 10));  // Alice (ID 1) as the source

        // Print the results: the shortest distance from Alice to each other person
        result.print();
    }
}
/*
(1,0.0)    // Alice is 0 distance away from herself
(2,1.0)    // Bob is 1 unit away from Alice
(3,2.0)    // Charlie is 2 units away from Alice (via Bob)
(4,4.0)    // David is 4 units away from Alice (via Charlie)
 */
