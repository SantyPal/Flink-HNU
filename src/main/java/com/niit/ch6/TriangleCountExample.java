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
        // Create the graph using Tuple2<Long, Long> (source, target vertices)
        // Each Tuple2 represents a friendship between two people
        Graph<Long, NullValue, NullValue> graph = Graph.fromTuple2DataSet(
                env.fromElements(
                        Tuple2.of(1L, 2L),  // Alice and Bob are friends
                        Tuple2.of(2L, 3L),  // Bob and Charlie are friends
                        Tuple2.of(3L, 1L),  // Charlie and Alice are friends (forming a triangle)
                        Tuple2.of(3L, 4L),  // Charlie and David are friends
                        Tuple2.of(4L, 5L),  // David and Emma are friends
                        Tuple2.of(1L, 3L),  // Alice and Charlie (already added, to form a triangle)
                        Tuple2.of(2L, 4L)   // Bob and David (adds more connectivity)
                ),
                env
        );

        // Run the TriangleEnumerator to find triangles and map to Tuple3<Long, Long, Integer>
        // Run the TriangleEnumerator to find triangles (sets of 3 friends)
        DataSet<Tuple3<Long, Long, Integer>> triangleCounts = graph
                .run(new TriangleEnumerator<Long, NullValue, NullValue>())
                .map(new org.apache.flink.api.common.functions.MapFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Integer>>() {
                    @Override
                    public Tuple3<Long, Long, Integer> map(Tuple3<Long, Long, Long> triangle) throws Exception {
                        // Map to Tuple3<Long, Long, Integer>, where the third element is always 1
                        // For each triangle, output the pair of people and a count of 1
                        return new Tuple3<>(triangle.f0, triangle.f1, 1);
                    }
                });

        // Print the triangle counts
        // Print the triangles found (person pairs and count of triangles)
        triangleCounts.print();
    }
}

/*
(1,2,1)  // Alice and Bob are part of a triangle
(2,3,1)  // Bob and Charlie are part of a triangle
(1,3,1)  // Alice and Charlie are part of a triangle
 */