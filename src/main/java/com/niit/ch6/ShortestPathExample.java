//package com.niit.ch6;
//
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.graph.Graph;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.graph.Edge;
//import org.apache.flink.graph.Vertex;
//
//
//import java.util.Arrays;
//import java.util.List;
//
//public class ShortestPathExample {
//    public static void main(String[] args) throws Exception {
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        // Sample vertices and edges
//        DataSet<Vertex<Long, String>> vertices = env.fromElements(
//                new Vertex<>(1L, "A"),
//                new Vertex<>(2L, "B"),
//                new Vertex<>(3L, "C")
//        );
//
//        DataSet<Edge<Long, Double>> edges = env.fromElements(
//                new Edge<>(1L, 2L, 1.0),
//                new Edge<>(2L, 3L, 2.0),
//                new Edge<>(1L, 3L, 2.5)
//        );
//
//        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);
//
//        // Run ShortestPaths algorithm
//        DataSet<Tuple2<Long, Double>> shortestPaths = ShortestPaths.compute(graph, 1L);
//
//        shortestPaths.print();
//    }
//}
