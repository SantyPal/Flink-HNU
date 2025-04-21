package com.niit.ch6;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UnDirectedWithName {
    public static void main(String[] args) throws Exception {
        // Set up execution environment for batch mode
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample vertices: (ID, Value) - nodes on a social platform
        DataSet<Vertex<Long, String>> vertices = env.fromElements(
                new Vertex<>(1L, "alice"),
                new Vertex<>(2L, "bob"),
                new Vertex<>(3L, "charlie"));

        // Sample edges: (Source ID, Target ID, Edge Value)
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 0.5), // Alice follows Bob normal close
                new Edge<>(2L, 3L, 1.0), // Bob follows Charlie - closely
                new Edge<>(1L, 3L, 0.8)  // Alice follows Charlie - less close
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

        // Create a mapping of vertex ID to vertex name
        DataSet<Map<Long, String>> vertexNameMap = vertices
                .map(new MapFunction<Vertex<Long, String>, Map<Long, String>>() {
                    @Override
                    public Map<Long, String> map(Vertex<Long, String> vertex) {
                        Map<Long, String> map = new HashMap<>();
                        map.put(vertex.getId(), vertex.getValue());
                        return map;
                    }
                })
                .reduce(new ReduceFunction<Map<Long, String>>() {
                    @Override
                    public Map<Long, String> reduce(Map<Long, String> map1, Map<Long, String> map2) {
                        map1.putAll(map2);
                        return map1;
                    }
                });

        // Collect the final map of vertex names in the main thread
        Map<Long, String> finalVertexNameMap = vertexNameMap.collect().get(0);  // This will collect the map into the main thread

        // Now print the edges with vertex names using the collected name map
        graph.getEdges()
                .flatMap(new FlatMapFunction<Edge<Long, Double>, String>() {
                    @Override
                    public void flatMap(Edge<Long, Double> edge, Collector<String> collector) throws Exception {
                        // Look up the vertex names directly from the finalVertexNameMap
                        String sourceName = finalVertexNameMap.get(edge.getSource());
                        String targetName = finalVertexNameMap.get(edge.getTarget());
                        collector.collect(sourceName + " is connected to " + targetName);
                    }
                })
                .print();
    }
}
