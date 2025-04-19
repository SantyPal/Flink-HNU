package com.niit.ch6;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class GellyExample {
    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Sample vertices: (ID, Value)
        DataSet<Vertex<Long, String>> vertices = env.fromElements(
                new Vertex<>(1L, "foo"),
                new Vertex<>(2L, "bar"),
                new Vertex<>(3L, "baz")
        );

        // Sample edges: (Source ID, Target ID, Edge Value)
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 0.5),
                new Edge<>(2L, 3L, 1.0),
                new Edge<>(1L, 3L, 0.8)
        );

        // Create the graph
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        // Map vertex values to uppercase
        Graph<Long, String, Double> updatedGraph = graph.mapVertices(
                new MapFunction<Vertex<Long, String>, String>() {
                    @Override
                    public String map(Vertex<Long, String> vertex) {
                        return vertex.getValue().toUpperCase();
                    }
                }
        );

        // Print transformed vertices
        updatedGraph.getVertices().print();
    }
}
