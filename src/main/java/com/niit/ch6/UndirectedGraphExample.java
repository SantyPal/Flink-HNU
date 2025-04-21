package com.niit.ch6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.api.java.tuple.Tuple2;

public class UndirectedGraphExample {
    public static void main(String[] args) throws Exception {
        // Set up execution environment for batch mode
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample vertices: (ID, Value) - nodes on a social platform
        DataSet<Vertex<Long, String>> vertices = env.fromElements(
                new Vertex<>(1L, "alice"),
                new Vertex<>(2L, "bob"),
                new Vertex<>(3L, "charlie"));

        // Sample edges: (Source ID, Target ID, Edge Value)
        // Directed edges (Alice follows Bob, Bob follows Charlie, etc.)
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 0.5), // Alice follows Bob
                new Edge<>(2L, 3L, 1.0), // Bob follows Charlie
                new Edge<>(1L, 3L, 0.8)  // Alice follows Charlie
        );

        // For undirected graph, create both directions for each edge
        DataSet<Edge<Long, Double>> undirectedEdges = edges
                .union(edges.map(new MapFunction<Edge<Long, Double>, Edge<Long, Double>>() {
                    @Override
                    public Edge<Long, Double> map(Edge<Long, Double> edge) throws Exception {
                        return new Edge<>(edge.getTarget(), edge.getSource(), edge.getValue());
                    }
                }));
        // Create the graph from vertices and undirected edges
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, undirectedEdges, env);

        // Print the vertices (names)
        graph.getVertices().print();

        // Print the edges (connections between users)
        graph.getEdges()

                .map(edge -> edge.getSource() + " is connected to " + edge.getTarget())
                .print();
    }
}
