package com.niit.ch6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map; // Import for Map
import java.util.HashMap;

public class Directed {
    public static void main(String[] args) throws Exception {
        // Set up execution environment for batch mode
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample vertices: (ID, Value) - nodes on a social platform
        DataSet<Vertex<Long, String>> vertices = env.fromElements(
                new Vertex<>(1L, "alice"),
                new Vertex<>(2L, "bob"),
                new Vertex<>(3L, "charlie"));

        // Sample edges: (Source ID, Target ID, Edge Value)
        // Edges: Directional relationships representing "follows"
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 0.5), // Alice follows Bob
                new Edge<>(2L, 3L, 1.0), // Bob follows Charlie
                new Edge<>(1L, 3L, 0.8)  // Alice follows Charlie
        );

        // Create the graph from people and "follows" relationships (directed)
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        // Broadcast the vertex names (this will be available on all nodes)
        DataSet<Tuple2<Long, String>> vertexNames = vertices
                .map(new MapFunction<Vertex<Long, String>, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(Vertex<Long, String> vertex) throws Exception {
                        return Tuple2.of(vertex.getId(), vertex.getValue());
                    }
                });

        Map<Long, String> vertexMap = new HashMap<>();
        for (Tuple2<Long, String> vertex : vertexNames.collect()) {
            vertexMap.put(vertex.f0, vertex.f1);
        }

        // Broadcast the vertex map to all operators
        final Map<Long, String> finalVertexMap = vertexMap;
        graph.getEdges()
                .map(new RichMapFunction<Edge<Long, Double>, String>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // No-op for now
                    }

                    @Override
                    public String map(Edge<Long, Double> edge) throws Exception {
                        // Get the source and target vertex IDs
                        Long sourceId = edge.getSource();
                        Long targetId = edge.getTarget();

                        // Fetch the source and target names using the broadcasted map
                        String sourceName = finalVertexMap.get(sourceId);
                        String targetName = finalVertexMap.get(targetId);

                        return sourceName + " follows " + targetName;
                    }

                    @Override
                    public void close() throws Exception {
                        // No-op for now
                    }
                })
                .print();
    }
}
