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

        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 1.0),
                new Edge<>(2L, 3L, 1.0),
                new Edge<>(1L, 3L, 4.0),
                new Edge<>(3L, 4L, 2.0)
        );

        Graph<Long, Double, Double> graph = Graph.fromDataSet(edges, vertexId -> Double.POSITIVE_INFINITY, env);

        DataSet<Vertex<Long, Double>> result = graph
                .run(new SingleSourceShortestPaths<>(1L, 10));

        result.print();
    }
}
