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

        // Define friendship connections (edges) — undirected connections
        /*
                        Vertices (1L to 8L) = People:
                1 → Alice
                2 → Bob
                3 → Charlie
                4 → David
                5 → Emma
                6 → Frank
                7 → Grace
                8 → Hannah
                         */
        DataSet<Edge<Long, NullValue>> edges = env.fromElements(
                new Edge<>(1L, 2L, NullValue.getInstance()), // Alice - Bob
                new Edge<>(2L, 3L, NullValue.getInstance()), // Bob - Charlie
                new Edge<>(4L, 5L, NullValue.getInstance()), // David - Emma
                new Edge<>(6L, 7L, NullValue.getInstance()), // Frank - Grace
                new Edge<>(7L, 8L, NullValue.getInstance())  // Grace - Hannah
        );

        // Create the graph, assigning each vertex ID as its own initial value (type Long)
        // Create a graph with vertex value = its ID (needed for algorithm)
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

        // Run Connected Components algorithm (max iterations = 10)
        DataSet<Vertex<Long, Long>> verticesWithComponentId =
                graph.run(new ConnectedComponents<>(10));

        // Convert to Tuple2 for printing - // Convert results to (PersonID, ComponentID) pairs for printing
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
    /*
(1,1)   // Alice is in group 1
(2,1)   // Bob is in group 1
(3,1)   // Charlie is in group 1
(4,4)   // David is in group 4
(5,4)   // Emma is in group 4
(6,6)   // Frank is in group 6
(7,6)   // Grace is in group 6
(8,6)   // Hannah is in group 6
 */
}