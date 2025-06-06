package com.niit.ch6;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;

public class GellyExample {
    public static void main(String[] args) throws Exception {
        // Set up execution environment for batch mode
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // Sample vertices: (ID, Value) - nodes on a social platform
        DataSet<Vertex<Long, String>> vertices = env.fromElements(
                new Vertex<>(1L, "alice"),
                new Vertex<>(2L, "bob"),
                new Vertex<>(3L, "charlie"));

        // Sample edges: (Source ID, Target ID, Edge Value)
        // Edges: Relationships with "friendship strength" (0.0 to 1.0)
      //  weights representing how close they are.
        DataSet<Edge<Long, Double>> edges = env.fromElements(
                new Edge<>(1L, 2L, 0.0), // Alice - Bob (normal relation)
                new Edge<>(2L, 3L, 1.0), // Bob - Charlie (highest relation)
                new Edge<>(1L, 3L, 0.8) // Alice - Charlie (Good Relation)
        );
        // Create the graph from people and friendships
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);
        graph.getVertices().print();

        // Example: Convert all names to uppercase (like processing user data)

        DataSet<String> VV=vertices.map(new MapFunction<Vertex<Long, String>, String>() {
            @Override
            public String map(Vertex<Long, String> ver_value) throws Exception {
                return ver_value.getValue().toUpperCase();
            }
        });
        System.out.println("Vertex Values");
        VV.print();

    }
}
