package com.niit.ch6;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.api.common.functions.MapFunction;

public class DirectedGraphExample {
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
                new Edge<>(3L, 1L, 0.8)  // Alice follows Charlie
        );
        // Create the directed graph from vertices and directed edges
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        // Print the vertices (names)
        graph.getVertices().print();

        // Join edges with vertices to get source and target names
        graph.getEdges()        //source
                .join(graph.getVertices()) // Join edges with source vertices           //1L, alice, 1L
                .where(edge -> edge.getSource()) // Use source ID for the join
                .equalTo(vertex -> vertex.getId()) // Join with vertex ID
                .with(new JoinFunction<Edge<Long, Double>, Vertex<Long, String>, Tuple2<Edge<Long, Double>, Vertex<Long, String>>>() {
                    @Override
                    public Tuple2<Edge<Long, Double>, Vertex<Long, String>> join(Edge<Long, Double> edge, Vertex<Long, String> vertex) throws Exception {
                        return new Tuple2<>(edge, vertex); // Pair the edge with the source vertex
                    }
                })
                .join(graph.getVertices()) // Join with target vertices
                .where(tuple -> tuple.f0.getTarget()) // Use target ID for the join
                .equalTo(vertex -> vertex.getId()) // Join with vertex ID
                .with(new JoinFunction<Tuple2<Edge<Long, Double>, Vertex<Long, String>>, Vertex<Long, String>, String>() {
                    @Override
                    public String join(Tuple2<Edge<Long, Double>, Vertex<Long, String>> edgeVertexTuple, Vertex<Long, String> targetVertex) throws Exception {
                        // Format the directed relationship
                        return edgeVertexTuple.f1.getValue() + " follows " + targetVertex.getValue();
                    }
                })
                .print();
    }
}
