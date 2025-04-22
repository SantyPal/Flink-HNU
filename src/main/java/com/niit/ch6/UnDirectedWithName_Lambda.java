package com.niit.ch6;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UnDirectedWithName_Lambda {
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
                .union(edges
                        .map(edge -> new Edge<>(edge.getTarget(), edge.getSource(), edge.getValue()))
                        .returns(TypeInformation.of(new TypeHint<Edge<Long, Double>>() {}))
                );

        // Create the graph from vertices and undirected edges
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, undirectedEdges, env);

        // Create a mapping of vertex ID to vertex name
        DataSet<Map<Long, String>> vertexNameMap = vertices
                .map(vertex -> {
                    Map<Long, String> map = new HashMap<>();
                    map.put(vertex.getId(), vertex.getValue());
                    return map;
                })
                .returns(TypeInformation.of(new TypeHint<Map<Long, String>>() {})) // explicitly specify the return type
                .reduce((map1, map2) -> {
                    map1.putAll(map2);
                    return map1;
                });
        // Collect the final map of vertex names in the main thread
        Map<Long, String> finalVertexNameMap = vertexNameMap.collect().get(0);  // This will collect the map into the main thread

        // Now print the edges with vertex names using the collected name map
        graph.getEdges()
                .flatMap((Edge<Long, Double> edge, Collector<String> out) -> {
                    String sourceName = finalVertexNameMap.get(edge.getSource());
                    String targetName = finalVertexNameMap.get(edge.getTarget());
                    out.collect(sourceName + " is connected to " + targetName);
                })
                .returns(String.class) // helps with type inference due to lambda
                .print();
    }
}
