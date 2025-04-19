package com.niit.ch6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.TriangleEnumerator;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.java.tuple.Tuple3;

public class TriangleCountExample {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Create the graph using Tuple2<Long, Long> (source, target vertices)
        Graph<Long, NullValue, NullValue> graph = Graph.fromTuple2DataSet(
                env.fromElements(
                        Tuple2.of(1L, 2L),
                        Tuple2.of(2L, 3L),
                        Tuple2.of(3L, 1L),
                        Tuple2.of(3L, 4L),
                        Tuple2.of(4L, 5L),
                        Tuple2.of(1L, 3L), // Add an extra edge to form a triangle
                        Tuple2.of(2L, 4L)  // Add an extra edge for more connectivity
                ),
                env
        );

        // Run the TriangleEnumerator to find triangles and map to Tuple3<Long, Long, Integer>
        DataSet<Tuple3<Long, Long, Integer>> triangleCounts = graph
                .run(new TriangleEnumerator<Long, NullValue, NullValue>())
                .map(new org.apache.flink.api.common.functions.MapFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Integer>>() {
                    @Override
                    public Tuple3<Long, Long, Integer> map(Tuple3<Long, Long, Long> triangle) throws Exception {
                        // Map to Tuple3<Long, Long, Integer>, where the third element is always 1
                        return new Tuple3<>(triangle.f0, triangle.f1, 1);
                    }
                });

        // Print the triangle counts
        triangleCounts.print();
    }
}
