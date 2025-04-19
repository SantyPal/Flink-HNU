package com.niit.ch6;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.ConnectedComponents;
import org.apache.flink.types.NullValue;

public class ConnectedComponentsExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create edges
        DataSet<Edge<Long, NullValue>> edges = env.fromElements(
                new Edge<>(1L, 2L, NullValue.getInstance()),
                new Edge<>(2L, 3L, NullValue.getInstance()),
                new Edge<>(4L, 5L, NullValue.getInstance()),
                new Edge<>(6L, 7L, NullValue.getInstance()),
                new Edge<>(7L, 8L, NullValue.getInstance())
        );

        // Create the graph, assigning each vertex ID as its own initial value (type Long)
        Graph<Long, Long, NullValue> graph = Graph.fromDataSet(
                edges,
                new MapFunction<Long, Long>() {
                    @Override
                    public Long map(Long id) {
                        return id; // assign initial vertex value equal to its ID
                    }
                },
                env
        );

        // Run Connected Components
        DataSet<Vertex<Long, Long>> verticesWithComponentId =
                graph.run(new ConnectedComponents<>(10));

        // Convert to Tuple2 for printing
        DataSet<Tuple2<Long, Long>> output = verticesWithComponentId
                .map(new MapFunction<Vertex<Long, Long>, Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> map(Vertex<Long, Long> vertex) {
                        return new Tuple2<>(vertex.getId(), vertex.getValue());
                    }
                });

        // Print the results
        output.print();
    }
}