//package com.niit.ch6;
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.graph.Edge;
//import org.apache.flink.graph.Graph;
//import org.apache.flink.graph.library.PageRank;
//import org.apache.flink.types.NullValue;
//
//public class GellyCSVExample {
//    public static void main(String[] args) throws Exception {
//        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//
//        // Read edges from CSV
//        String edgesPath = "path/to/edges.csv";
//        DataSet<Edge<Long, NullValue>> edges = env.readCsvFile(edgesPath)
//                .fieldDelimiter(",")
//                .types(Long.class, Long.class)
//                .map(tuple -> new Edge<>(tuple.f0, tuple.f1, NullValue.getInstance()));
//
//        // Create graph from DataSet of edges
//        Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(
//                edges,
//                env
//        );
//
//        // Configure PageRank
//        double dampingFactor = 0.85;
//        int maxIterations = 10;
//        PageRank<Long> pageRank = new PageRank<>(dampingFactor, maxIterations);
//
//        // Execute and print results
//        graph.run(pageRank).getVertices().print();
//    }
//}