//package com.niit.ch6;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.graph.Graph;
//import org.apache.flink.graph.library.ShortestPaths;
//import org.apache.flink.api.java.dataset.DataSet;
//
//import java.util.Arrays;
//import java.util.List;
//
//public class ShortestPathExample {
//    public static void main(String[] args) throws Exception {
//
//        // Create the execution environment
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        // Create the list of vertices
//        List<Tuple2<Long, String>> vertices = Arrays.asList(
//                new Tuple2<>(1L, "A"),
//                new Tuple2<>(2L, "B"),
//                new Tuple2<>(3L, "C"),
//                new Tuple2<>(4L, "D")
//        );
//
//        // Create the list of edges (source, target, edge weight)
//        List<Tuple3<Long, Long, Double>> edges = Arrays.asList(
//                new Tuple3<>(1L, 2L, 2.0),
//                new Tuple3<>(2L, 3L, 1.0),
//                new Tuple3<>(3L, 4L, 5.0),
//                new Tuple3<>(1L, 3L, 3.0),
//                new Tuple3<>(4L, 1L, 4.0)
//        );
//
//        // Convert the Lists to DataSets
//        DataSet<Tuple2<Long, String>> vertexDataSet = env.fromCollection(vertices);
//        DataSet<Tuple3<Long, Long, Double>> edgeDataSet = env.fromCollection(edges);
//
//        // Create the graph using vertices and edges DataSets
//        Graph<Long, String, Double> graph = Graph.fromDataSet(vertexDataSet, edgeDataSet, env);
//
//        // Compute the shortest paths using the ShortestPaths algorithm
//        // Let's assume that vertex 1 is the source vertex
//        DataSet<org.apache.flink.graph.library.ShortestPaths.Result> shortestPaths = graph
//                .run(new ShortestPaths<Long>(1L, 10)); // 1L is the source vertex, 10 is the number of paths
//
//        // Print the shortest paths
//        shortestPaths.print();
//    }
//}
